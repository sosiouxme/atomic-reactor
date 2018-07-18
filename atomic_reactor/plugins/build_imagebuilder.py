"""
Copyright (c) 2018 Red Hat, Inc
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
"""
from __future__ import print_function, unicode_literals

import subprocess
from six import PY2
import os

from atomic_reactor.util import get_exported_image_metadata
from atomic_reactor.plugin import BuildStepPlugin
from atomic_reactor.build import BuildResult
from atomic_reactor.constants import CONTAINER_IMAGEBUILDER_BUILD_METHOD
from atomic_reactor.constants import EXPORTED_SQUASHED_IMAGE_NAME, IMAGE_TYPE_DOCKER_ARCHIVE


class ImagebuilderPlugin(BuildStepPlugin):
    """
    Build image using imagebuilder https://github.com/openshift/imagebuilder
    This requires the imagebuilder executable binary to be in $PATH.
    """

    key = CONTAINER_IMAGEBUILDER_BUILD_METHOD

    def run(self):
        """
        Build image inside current environment using imagebuilder;
        It's expected this may run within (privileged) docker container.

        TODO: directly invoke go imagebuilder library in shared object via python module
              instead of running via subprocess.

        Returns:
            BuildResult
        """
        builder = self.workflow.builder
        image = builder.image.to_str()

        # set up subprocess params
        kwargs = dict(stderr=subprocess.STDOUT)
        encoding_params = dict(encoding='utf-8', errors='replace')
        if not PY2:
            kwargs.update(encoding_params)

        rc = 0
        try:
            out = subprocess.check_output(['imagebuilder', '-t', image, builder.df_dir], **kwargs)
        except subprocess.CalledProcessError as exc:
            out, rc = (exc.output, exc.returncode)

        out = out.decode(**encoding_params) if PY2 else out
        self.log.info('output from imagebuilder:\n%s', out)

        out_lines = out.splitlines(True)
        if rc != 0:
            # assume the last line holds an error msg; include it in the failure summary.
            err = out_lines[-1] if out_lines else "<imagebuilder had bad exit code but no output>"
            self.log.error("image build failed with rc=%d", rc)
            return BuildResult(
                logs=out_lines,
                fail_reason="image build failed (rc={}): {}".format(rc, err),
            )

        image_id = builder.get_built_image_info()['Id']
        if ':' not in image_id:
            # Older versions of the daemon do not include the prefix
            image_id = 'sha256:{}'.format(image_id)

        # since we need no squash, export the image for local operations like squash would have
        self.log.info("build succeeded. fetching image %s from docker", image)
        output_path = os.path.join(self.workflow.source.workdir, EXPORTED_SQUASHED_IMAGE_NAME)
        with open(output_path, "w") as image_file:
            image_file.write(self.tasker.d.get_image(image).data)
        img_metadata = get_exported_image_metadata(output_path, IMAGE_TYPE_DOCKER_ARCHIVE)
        self.workflow.exported_image_sequence.append(img_metadata)

        return BuildResult(logs=out_lines, image_id=image_id, skip_layer_squash=True)
