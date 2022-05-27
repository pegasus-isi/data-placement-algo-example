#!/usr/bin/env python3

import logging
from Pegasus.api import *
from datetime import datetime
from argparse import ArgumentParser
from pathlib import Path
from typing import List
import os

logging.basicConfig(level=logging.INFO)

props = Properties()
props["pegasus.mode"] = "development"
props.write()


class Example_WF(object):
    def __init__(self, input_files: List[str]):
        self.input_filenames = input_files
        self.input_files = []
        for input_file in input_files:
            inputFile = File(input_file).add_metadata(creator="sai")
            self.input_files.append(inputFile)

        print(self.input_filenames)
        print(self.input_files)

    def generate_tc(self) -> TransformationCatalog:
        # generate transformation catalog and then return tc object
        tc = TransformationCatalog()

        unzip = Transformation(
            "unzip",
            site="condorpool",
            pfn="/bin/tar",
            is_stageable=False,
        )

        tar = Transformation(
            "tar",
            site="condorpool",
            pfn="/bin/tar",
            is_stageable=False,
        )

        tc.add_transformations(tar)
        tc.add_transformations(unzip)

        return tc

    def generate_rc(self, files: List[str]) -> ReplicaCatalog:
        # generate replica catalog and then return rc object
        rc = ReplicaCatalog()

        for f in files:
            p = Path(f)
            input_filename = p.name
            rc.add_replica(
                "local", input_filename, Path("input/").resolve() / input_filename
            )

        return rc

    def create_sites_catalog(self, exec_site_name="condorpool"):
        sc = SiteCatalog()

        shared_scratch_dir = os.path.join(Path(".").resolve(), "scratch")
        local_storage_dir = os.path.join(Path(".").resolve(), "output")

        local = Site("local").add_directories(
            Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
                FileServer("file://" + shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, local_storage_dir).add_file_servers(
                FileServer("file://" + local_storage_dir, Operation.ALL)
            ),
        )

        exec_site = (
            Site(exec_site_name)
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

        sc.add_sites(local, exec_site)
        return sc

    def generate_workflow(self) -> Workflow:
        workflow = Workflow("example")

        tc = self.generate_tc()
        rc = self.generate_rc(self.input_filenames)

        print(self.input_filenames[0])
        print(self.input_filenames[1])

        t1 = (
            Job("tar", _id="t1")
            .add_args(
                "-czvf",
                "result.tar.gz",
                self.input_filenames[0],
                self.input_filenames[1],
            )
            .add_inputs(self.input_files[0], self.input_files[1])
            .add_pegasus_profile(label="top")
            .add_condor_profile(requirements='DC_ID == "dc-1"')
        )

        ds6_name = "ds6.txt"
        ds6 = File(ds6_name)

        t5 = (
            Job("unzip", _id="t5")
            .add_args("-zxvf", self.input_filenames[4])
            .add_inputs(self.input_files[4])
            .add_outputs(ds6)
            .add_pegasus_profile(label="top")
        )

        t2 = (
            Job("tar", _id="t2")
            .add_args(
                "-czvf",
                "result.tar.gz",
                self.input_filenames[0],
                self.input_filenames[1],
                ds6_name,
            )
            .add_inputs(self.input_files[0], self.input_files[1], ds6)
            .add_pegasus_profile(label="top")
        )

        t3 = (
            Job("tar", _id="t3")
            .add_args(
                "-czvf",
                "result.tar.gz",
                self.input_filenames[0],
                self.input_filenames[1],
                self.input_filenames[2],
                ds6_name,
            )
            .add_inputs(
                self.input_files[0], self.input_files[1], self.input_files[2], ds6
            )
            .add_pegasus_profile(label="top")
        )

        t4 = (
            Job("tar", _id="t4")
            .add_args(
                "-czvf",
                "ds6.tar.gz",
                self.input_filenames[2],
                self.input_filenames[3],
                ds6_name,
            )
            .add_inputs(self.input_files[2], self.input_files[3], ds6)
            .add_pegasus_profile(label="top")
        )

        workflow.add_jobs(t1, t2, t3, t4, t5)
        workflow.add_transformation_catalog(tc)
        workflow.add_replica_catalog(rc)
        workflow.add_site_catalog(self.create_sites_catalog())

        return workflow


if __name__ == "__main__":
    parser = ArgumentParser(description="Example Workflow")
    parser.add_argument(
        "-f",
        "--files",
        metavar="INPUT_FILES",
        type=str,
        nargs="+",
        help="Input Files",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="OUTPUT_FILE",
        type=str,
        default="workflow.yml",
        help="Output file name",
    )

    args = parser.parse_args()

    print(args.files)
    example_wf = Example_WF(args.files)
    workflow = example_wf.generate_workflow()
    workflow.write()
    workflow.graph(
        output="workflow.png", include_files=True, no_simplify=True, label="xform-id"
    )
    workflow.plan(submit=True)
    workflow.wait()
