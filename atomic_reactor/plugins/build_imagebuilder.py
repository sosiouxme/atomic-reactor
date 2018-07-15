"""
Copyright (c) 2018 Red Hat, Inc
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
"""
from __future__ import print_function, unicode_literals

import subprocess
import time
from six import PY2
import os
from fcntl import fcntl, F_GETFL, F_SETFL

from atomic_reactor.util import get_exported_image_metadata
from atomic_reactor.plugin import BuildStepPlugin
from atomic_reactor.build import BuildResult
from atomic_reactor.constants import CONTAINER_IMAGEBUILDER_BUILD_METHOD
from atomic_reactor.constants import EXPORTED_SQUASHED_IMAGE_NAME, IMAGE_TYPE_DOCKER_ARCHIVE


def make_nonblocking(stream):
    # set the O_NONBLOCK flag of file descriptor:
    flags = fcntl(stream, F_GETFL)
    fcntl(stream, F_SETFL, flags | os.O_NONBLOCK)




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

        Returns:
            BuildResult
        """
        builder = self.workflow.builder

        image = builder.image.to_str()
        # TODO: directly invoke go imagebuilder library in shared object via python module
        kwargs = dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        if not PY2:
            kwargs['encoding'] = 'utf-8'
        ib_process = subprocess.Popen(['imagebuilder', '-t', image, builder.df_dir], **kwargs)

        self.log.debug('imagebuilder build has begun; waiting for it to finish')
        output = []
        while True:
            poll = ib_process.poll()
            self.log.debug('poll is %s', poll)
            out = ib_process.stdout.readline()
            out = out.decode() if PY2 else out
            if out:
                self.log.info('%s', out.rstrip())
                output.append(out)
            else:
                self.log.debug('out is %s', out)
            if out == '' and poll is not None:
                break

        if ib_process.returncode != 0:
            # in the case of an apparent failure, single out the last line to
            # include in the failure summary.
            err = output[-1] if output else "<imagebuilder had bad exit code but no output>"
            return BuildResult(
                logs=output,
                fail_reason="image build failed (rc={}): {}".format(ib_process.returncode, err),
            )

        image_id = builder.get_built_image_info()['Id']
        if ':' not in image_id:
            # Older versions of the daemon do not include the prefix
            image_id = 'sha256:{}'.format(image_id)

        # since we need no squash, export the image for local operations like squash would have
        self.log.info("fetching image %s from docker", image)
        output_path = os.path.join(self.workflow.source.workdir, EXPORTED_SQUASHED_IMAGE_NAME)
        with open(output_path, "w") as image_file:
            image_file.write(self.tasker.d.get_image(image).data)
        img_metadata = get_exported_image_metadata(output_path, IMAGE_TYPE_DOCKER_ARCHIVE)
        self.workflow.exported_image_sequence.append(img_metadata)

        return BuildResult(logs=output, image_id=image_id, skip_layer_squash=True)
