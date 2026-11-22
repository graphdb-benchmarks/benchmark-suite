from subprocess import CompletedProcess
import unittest
from unittest.mock import patch

from shootout.container import ensure_image


class Container(unittest.TestCase):
    def test_build_target_matches_docker_host_architecture(self):
        for machine, arch, tugraph in (
            ("aarch64", "arm64", "tugraph/tugraph-runtime-arm64v8-centos7:4.5.1"),
            ("x86_64", "amd64", "tugraph/tugraph-runtime-centos7:4.5.2"),
        ):
            with self.subTest(machine=machine), \
                    patch("shootout.container.subprocess.check_output", return_value=f"linux {machine}\n"), \
                    patch("shootout.container.subprocess.run", side_effect=[
                        CompletedProcess([], 1),
                        CompletedProcess([], 0),
                        CompletedProcess([], 0, stdout=f"linux/{arch} image-id\n"),
                    ]) as run:
                self.assertEqual(ensure_image(["docker"]), (f"linux/{arch}", "image-id"))
                build = run.call_args_list[1].args[0]
                self.assertEqual(build[1:4], ["build", "--platform", f"linux/{arch}"])
                self.assertIn(f"TUGRAPH_IMAGE={tugraph}", build)
                self.assertIn(f"VELR_ARCH={machine}", build)

    def test_rejects_an_image_for_a_different_architecture(self):
        with patch("shootout.container.subprocess.check_output", return_value="linux aarch64\n"), \
                patch("shootout.container.subprocess.run", return_value=CompletedProcess(
                    [], 0, stdout="linux/amd64 image-id\n",
                )):
            with self.assertRaisesRegex(ValueError, "expected linux/arm64"):
                ensure_image(["docker"])
