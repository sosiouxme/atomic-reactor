"""
Copyright (c) 2017 Red Hat, Inc
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.

Takes the filesystem image created by the Dockerfile generated by
pre_flatpak_create_dockerfile, extracts the tree at /var/tmp/flatpak-build
and turns it into a Flatpak application or runtime.
"""

import os
from six.moves import configparser
import re
import shutil
import subprocess
import tarfile
from textwrap import dedent

from atomic_reactor.constants import IMAGE_TYPE_OCI, IMAGE_TYPE_OCI_TAR
from atomic_reactor.plugin import PrePublishPlugin
from atomic_reactor.plugins.pre_flatpak_create_dockerfile import get_flatpak_source_info
from atomic_reactor.rpm_util import parse_rpm_output
from atomic_reactor.util import get_exported_image_metadata


# Returns flatpak's name for the current arch
def get_arch():
    return subprocess.check_output(['flatpak', '--default-arch'],
                                   universal_newlines=True).strip()


# flatpak build-init requires the sdk and runtime to be installed on the
# build system (so that subsequent build steps can execute things with
# the SDK). While it isn't impossible to download the runtime image and
# install the flatpak, that would be a lot of unnecessary complexity
# since our build step is just unpacking the filesystem we've already
# created. This is a stub implementation of 'flatpak build-init' that
# doesn't check for the SDK or use it to set up the build filesystem.
def build_init(directory, appname, sdk, runtime, runtime_branch):
    if not os.path.isdir(directory):
        os.mkdir(directory)
    with open(os.path.join(directory, "metadata"), "w") as f:
        f.write(dedent("""\
                       [Application]
                       name={appname}
                       runtime={runtime}/{arch}/{runtime_branch}
                       sdk={sdk}/{arch}/{runtime_branch}
                       """.format(appname=appname,
                                  sdk=sdk,
                                  runtime=runtime,
                                  runtime_branch=runtime_branch,
                                  arch=get_arch())))
    os.mkdir(os.path.join(directory, "files"))


# add_app_prefix('org.gimp', 'gimp, 'gimp.desktop') => org.gimp.desktop
# add_app_prefix('org.gnome', 'eog, 'eog.desktop') => org.gnome.eog.desktop
def add_app_prefix(app_id, root, full):
    prefix = app_id
    if prefix.endswith('.' + root):
        prefix = prefix[:-(1 + len(root))]
    return prefix + '.' + full


def find_desktop_files(builddir):
    desktopdir = os.path.join(builddir, 'files/share/applications')
    for (dirpath, dirnames, filenames) in os.walk(desktopdir):
        for filename in filenames:
            if filename.endswith('.desktop'):
                yield os.path.join(dirpath, filename)


def find_icons(builddir, name):
    icondir = os.path.join(builddir, 'files/share/icons/hicolor')
    for (dirpath, dirnames, filenames) in os.walk(icondir):
        for filename in filenames:
            if filename.startswith(name + '.'):
                yield os.path.join(dirpath, filename)


def update_desktop_files(app_id, builddir):
    for full_path in find_desktop_files(builddir):
        cp = configparser.RawConfigParser()
        cp.read([full_path])
        try:
            icon = cp.get('Desktop Entry', 'Icon')
        except configparser.NoOptionError:
            icon = None

        # Does it have an icon?
        if icon and not icon.startswith(app_id):
            found_icon = False

            # Rename any matching icons
            for icon_file in find_icons(builddir, icon):
                shutil.copy(icon_file,
                            os.path.join(os.path.dirname(icon_file),
                                         add_app_prefix(app_id, icon, os.path.basename(icon_file))))
                found_icon = True

            # If we renamed the icon, change the desktop file
            if found_icon:
                subprocess.check_call(['desktop-file-edit',
                                       '--set-icon',
                                       add_app_prefix(app_id, icon, icon), full_path])

        # Is the desktop file not prefixed with the app id, then prefix it
        basename = os.path.basename(full_path)
        if not basename.startswith(app_id):
            shutil.move(full_path,
                        os.path.join(os.path.dirname(full_path),
                                     add_app_prefix(app_id,
                                                    basename[:-len('.desktop')],
                                                    basename)))


class FlatpakCreateOciPlugin(PrePublishPlugin):
    key = 'flatpak_create_oci'
    is_allowed_to_fail = False

    def __init__(self, tasker, workflow):
        """
        :param tasker: DockerTasker instance
        :param workflow: DockerBuildWorkflow instance
        """
        super(FlatpakCreateOciPlugin, self).__init__(tasker, workflow)

    # Compiles a list of path mapping rules to a simple function that matches
    # against a list of fixed patterns, see below for rule syntax
    def _compile_target_rules(rules):
        ROOT = "var/tmp/flatpak-build"

        patterns = []
        for source, target in rules:
            source = re.sub("^ROOT", ROOT, source)
            if source.endswith("/"):
                patterns.append((re.compile(source + "(.*)"), target, False))
                patterns.append((source[:-1], target, True))
            else:
                patterns.append((source, target, True))

        def get_target_func(self, path):
            for source, target, is_exact_match in patterns:
                if is_exact_match:
                    if source == path:
                        return target
                else:
                    m = source.match(path)
                    if m:
                        return os.path.join(target, m.group(1))

            return None

        return get_target_func

    # Rules for mapping paths within the exported filesystem image to their
    # location in the final flatpak filesystem
    #
    # ROOT = /var/tmp/flatpak-build
    # No trailing slash - map a directory itself exactly
    # trailing slash - map a directory and everything inside of it

    _get_target_path_runtime = _compile_target_rules([
        # We need to make sure that 'files' is created before 'files/etc',
        # which wouldn't happen if just relied on ROOT/usr/ => files.
        # Instead map ROOT => files and omit ROOT/usr
        ("ROOT", "files"),
        ("ROOT/usr", None),

        # We map ROOT/usr => files and ROOT/etc => files/etc. This creates
        # A conflict between ROOT/usr/etc and /ROOT/etc. Just assume there
        # is nothing useful in /ROOT/usr/etc.
        ("ROOT/usr/etc/", None),

        ("ROOT/usr/", "files"),
        ("ROOT/etc/", "files/etc")
    ])

    _get_target_path_app = _compile_target_rules([
        ("ROOT/app/", "files")
    ])

    def _get_target_path(self, export_path):
        if self.source.runtime:
            return self._get_target_path_runtime(export_path)
        else:
            return self._get_target_path_app(export_path)

    def _export_container(self, container_id):
        outfile = os.path.join(self.workflow.source.workdir, 'filesystem.tar.gz')
        manifestfile = os.path.join(self.workflow.source.workdir, 'flatpak-build.rpm_qf')

        export_stream = self.tasker.d.export(container_id)
        out_fileobj = open(outfile, "wb")
        compress_process = subprocess.Popen(['gzip', '-c'],
                                            stdin=subprocess.PIPE,
                                            stdout=out_fileobj)
        in_tf = tarfile.open(fileobj=export_stream, mode='r|')
        out_tf = tarfile.open(fileobj=compress_process.stdin, mode='w|')

        for member in in_tf:
            if member.name == 'var/tmp/flatpak-build.rpm_qf':
                reader = in_tf.extractfile(member)
                with open(manifestfile, 'wb') as out:
                    out.write(reader.read())
                reader.close()
            target_name = self._get_target_path(member.name)
            if target_name is None:
                continue

            # Match the ownership/permissions changes done by 'flatpak build-export'.
            # See commit_filter() in:
            #   https://github.com/flatpak/flatpak/blob/master/app/flatpak-builtins-build-export.c
            #
            # We'll run build-export anyways in the app case, but in the runtime case we skip
            # flatpak build-export and use ostree directly.
            member.uid = 0
            member.gid = 0
            member.uname = "root"
            member.gname = "root"

            if member.isdir():
                member.mode = 0o0755
            elif member.mode & 0o0100:
                member.mode = 0o0755
            else:
                member.mode = 0o0644

            member.name = target_name
            if member.islnk():
                # Hard links have full paths within the archive (no leading /)
                link_target = self._get_target_path(member.linkname)
                if link_target is None:
                    self.log.debug("Skipping %s, hard link to %s", target_name, link_target)
                    continue
                member.linkname = link_target
                out_tf.addfile(member)
            elif member.issym():
                # Symlinks have the literal link target, which will be
                # relative to the chroot and doesn't need rewriting
                out_tf.addfile(member)
            else:
                f = in_tf.extractfile(member)
                out_tf.addfile(member, fileobj=f)

        in_tf.close()
        out_tf.close()
        export_stream.close()
        compress_process.stdin.close()
        if compress_process.wait() != 0:
            raise RuntimeError("gzip failed")
        out_fileobj.close()

        return outfile, manifestfile

    def _export_filesystem(self):
        image = self.workflow.image
        self.log.info("Creating temporary docker container")
        container_dict = self.tasker.d.create_container(image)
        container_id = container_dict['Id']

        try:
            return self._export_container(container_id)
        finally:
            self.log.info("Cleaning up docker container")
            self.tasker.d.remove_container(container_id)

    def _get_components(self, manifest):
        with open(manifest, 'r') as f:
            lines = f.readlines()

        return parse_rpm_output(lines)

    def _check_runtime_manifest(self, components):
        # For a runtime, we want to make sure that the set of RPMs that was installed
        # into the filesystem is *exactly* the set that is listed in the 'runtime'
        # profile. Requiring the full listed set of RPMs to be listed makes it
        # easier to catch unintentional changes in the package list that might break
        # applications depending on the runtime. It also simplifies the checking we
        # do for application flatpaks, since we can simply look at the runtime
        # modulemd to find out what packages are present in the runtime.

        base_module = self.source.compose.base_module

        component_names = {c['name'] for c in components}
        expected_component_names = set(base_module.mmd.profiles['runtime'].rpms)

        if component_names != expected_component_names:
            missing = expected_component_names - component_names
            extra = component_names - expected_component_names
            raise RuntimeError("Installed set of packages does not match runtime profile:\n"
                               "\tmissing: {}\n\textra: {}"
                               .format(" ".join(sorted(missing)),
                                       " ".join(sorted(extra))))

        return components

    def _identify_app_source_modules(self):
        modules = self.source.compose.modules
        base_module = self.source.compose.base_module

        # Identify the module for the Flatpak runtime that this app runs against
        runtime_module = None
        for key in base_module.mmd.buildrequires.keys():
            try:
                module = modules[key]
                if 'runtime' in module.mmd.profiles:
                    runtime_module = module
                    break
            except KeyError:
                pass

        if runtime_module is None:
            raise RuntimeError("Failed to identify runtime module in the buildrequires for {}"
                               .format(base_module.name))

        # Identify all modules that were build against the Flatpak runtime,
        # and thus were built with prefix=/app. This is primarily the app module
        # but might contain modules shared between multiple flatpaks as well.
        app_modules = [m for m in modules.values() if runtime_module.name in m.mmd.buildrequires]

        assert base_module in app_modules

        return runtime_module, app_modules

    def _check_app_manifest(self, components):
        # For an application, we want to make sure that each RPM that was installed
        # into the filesystem is *either* an RPM that is part of the 'runtime'
        # profile of the base runtime, or from a module that was built with
        # flatpak-rpm-macros in the install root and, thus, prefix=/app.

        runtime_module, app_modules = self._identify_app_source_modules()

        app_components = []
        stray_components = []

        runtime_rpms = runtime_module.mmd.profiles['runtime'].rpms
        for component in components:
            # Is it from the runtime?
            if component['name'] in runtime_rpms:
                continue

            # If it's not from the runtime, check that the specific package
            # version was built in one of the app modules
            if component['epoch'] is not None:
                component_filename = \
                    "{name}-{epoch}:{version}-{release}.{arch}.rpm".format(**component)
            else:
                # The PDC data has the misapprehension that epoch 0 is the same as no epoch
                component_filename = \
                    "{name}-0:{version}-{release}.{arch}.rpm".format(**component)

            found_in_app = False
            for app_module in app_modules:
                if component_filename in app_module.rpms:
                    found_in_app = True
                    break

            if found_in_app:
                app_components.append(component)
                continue

            stray_components.append(component_filename)

        if len(stray_components) > 0:
            raise RuntimeError("Found installed packages not from the runtime or application: {}"
                               .format(" ".join(stray_components)))

        return app_components

    def _create_runtime_oci(self, tarred_filesystem, outfile):
        info = self.source.flatpak_json

        builddir = os.path.join(self.workflow.source.workdir, "build")
        os.mkdir(builddir)

        repo = os.path.join(self.workflow.source.workdir, "repo")
        subprocess.check_call(['ostree', 'init', '--mode=archive-z2', '--repo', repo])

        runtime_id = info['runtime']
        runtime_version = info['runtime-version']

        args = {
            'runtime_id': runtime_id,
            'arch': get_arch(),
            'runtime_version': runtime_version
        }

        METADATA_TEMPLATE = dedent("""\
            [Runtime]
            name={runtime_id}
            runtime={runtime_id}/{arch}/{runtime_version}
            sdk={runtime_id}/{arch}/{runtime_version}

            [Environment]
            LD_LIBRARY_PATH=/app/lib64:/app/lib
            GI_TYPELIB_PATH=/app/lib64/girepository-1.0
            """)

        with open(os.path.join(builddir, 'metadata'), 'w') as f:
            f.write(METADATA_TEMPLATE.format(**args))

        runtime_ref = 'runtime/{runtime_id}/{arch}/{runtime_version}'.format(**args)

        subprocess.check_call(['ostree', 'commit',
                               '--repo', repo, '--owner-uid=0',
                               '--owner-gid=0', '--no-xattrs',
                               '--branch', runtime_ref,
                               '-s', 'build of ' + runtime_ref,
                               '--tree=tar=' + tarred_filesystem,
                               '--tree=dir=' + builddir])
        subprocess.check_call(['ostree', 'summary', '-u', '--repo', repo])

        subprocess.check_call(['flatpak', 'build-bundle', repo,
                               '--oci', '--runtime',
                               outfile, runtime_id, runtime_version])

        return runtime_ref

    def _create_app_oci(self, tarred_filesystem, outfile):
        info = self.source.flatpak_json
        app_id = info['id']

        runtime_id = info['runtime']
        runtime_version = info['runtime-version']

        builddir = os.path.join(self.workflow.source.workdir, "build")
        os.mkdir(builddir)

        repo = os.path.join(self.workflow.source.workdir, "repo")

        # See comment for build_init() for why we can't use 'flatpak build-init'
        # subprocess.check_call(['flatpak', 'build-init',
        #                        builddir, app_id, runtime_id, runtime_id, runtime_version])
        build_init(builddir, app_id, runtime_id, runtime_id, runtime_version)

        # with gzip'ed tarball, tar is several seconds faster than tarfile.extractall
        subprocess.check_call(['tar', 'xCfz', builddir, tarred_filesystem])

        update_desktop_files(app_id, builddir)

        subprocess.check_call(['flatpak', 'build-finish'] + info['finish-args'] + [builddir])
        subprocess.check_call(['flatpak', 'build-export', repo, builddir])

        subprocess.check_call(['flatpak', 'build-bundle', repo, '--oci', outfile, app_id])

        app_ref = 'app/{app_id}/{arch}/master'.format(app_id=app_id, arch=get_arch())

        return app_ref

    def run(self):
        self.source = get_flatpak_source_info(self.workflow)
        if self.source is None:
            raise RuntimeError("flatpak_create_dockerfile must be run before flatpak_create_oci")

        tarred_filesystem, manifest = self._export_filesystem()
        self.log.info('filesystem tarfile written to %s', tarred_filesystem)
        self.log.info('manifest written to %s', manifest)

        all_components = self._get_components(manifest)
        if self.source.runtime:
            image_components = self._check_runtime_manifest(all_components)
        else:
            image_components = self._check_app_manifest(all_components)

        self.log.info("Components:\n%s",
                      "\n".join("        {name}-{epoch}:{version}-{release}.{arch}.rpm"
                                .format(**c) for c in image_components))

        self.workflow.image_components = image_components

        outfile = os.path.join(self.workflow.source.workdir, 'flatpak-oci-image')

        if self.source.runtime:
            ref_name = self._create_runtime_oci(tarred_filesystem, outfile)
        else:
            ref_name = self._create_app_oci(tarred_filesystem, outfile)

        metadata = get_exported_image_metadata(outfile, IMAGE_TYPE_OCI)
        metadata['ref_name'] = ref_name
        self.workflow.exported_image_sequence.append(metadata)

        self.log.info('OCI image is available as %s', outfile)

        tarred_outfile = outfile + '.tar'
        with tarfile.TarFile(tarred_outfile, "w") as tf:
            for f in os.listdir(outfile):
                tf.add(os.path.join(outfile, f), f)

        metadata = get_exported_image_metadata(tarred_outfile, IMAGE_TYPE_OCI_TAR)
        metadata['ref_name'] = ref_name
        self.workflow.exported_image_sequence.append(metadata)

        self.log.info('OCI tarfile is available as %s', tarred_outfile)
