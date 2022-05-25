#!/usr/bin/env python3

import logging
from Pegasus.api import *
from datetime import datetime
from argparse import ArgumentParser
from pathlib import Path
from typing import List

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
        
    def generate_tc(self) -> None:
        # generate transformation catalog and then return tc object
        tc = TransformationCatalog()

        unzip = Transformation(
            "gunzip",
            site="condorpool",
            pfn="/bin/gunzip",
            is_stageable=False,
        )  
        
        zip = Transformation(
            "zip",
            site="condorpool",
            pfn="/bin/gzip",
            is_stageable=False,
        )   
        
        tc.add_transformations(zip)
        tc.add_transformations(unzip)

        return tc 

    def generate_rc(self, files: List[str]) -> ReplicaCatalog:
        # generate replica catalog and then return rc object
        rc = ReplicaCatalog()

        for f in files:
            p = Path(f)
            input_filename = p.name
            rc.add_replica("local", input_filename, Path("input/").resolve() / input_filename)

        return rc

    def generate_workflow(self) -> Workflow:
        workflow = Workflow("example")

        tc = self.generate_tc()
        rc = self.generate_rc(self.input_files)
        
        # omitting .add_inputs(*radar_inputs) because we don't want Pegasus to "track" these files
        t1 = Job("tar", _id="tar").add_args("-czvf", "result.tar.gz", self.input_filenames[0], self.input_filenames[1])\
                            .add_inputs(self.input_files[0], self.input_files[1]) \
                            .add_condor_profile(requirements="MACHINE_SPECIAL_ID == 1")\
                            .add_pegasus_profile(label="top")

        ds6_name = "ds6.txt"
        ds6 = File(ds6_name)

        t5 = Job("gzip", _id="gzip").add_args(self.input_filenames[4])\
                            .add_inputs(self.input_files[4]) \
                            .add_outputs(ds6) \
                            .add_condor_profile(requirements="MACHINE_SPECIAL_ID == 1")\
                            .add_pegasus_profile(label="top")

        t2 = Job("tar", _id="tar").add_args("-czvf", "result.tar.gz", self.input_filenames[0], self.input_filenames[1], ds6_name)\
                    .add_inputs(self.input_files[0], self.input_files[1], ds6) \
                    .add_condor_profile(requirements="MACHINE_SPECIAL_ID == 1")\
                    .add_pegasus_profile(label="top")

        t3 = Job("tar", _id="tar").add_args("-czvf", "result.tar.gz", self.input_filenames[0], self.input_filenames[1], self.input_filenames[2], ds6_name)\
                    .add_inputs(self.input_files[0], self.input_files[1], self.input_files[2], ds6) \
                    .add_condor_profile(requirements="MACHINE_SPECIAL_ID == 1")\
                    .add_pegasus_profile(label="top")

        t4 = Job("tar", _id="tar").add_args("-czvf", "ds6.tar.gz", self.input_filenames[2], self.input_filenames[3], ds6_name)\
                    .add_inputs(self.input_files[2], self.input_files[3], ds6) \
                    .add_condor_profile(requirements="MACHINE_SPECIAL_ID == 1")\
                    .add_pegasus_profile(label="top")

        
        workflow.add_jobs(t1, t2, t3, t4, t5)
        workflow.add_transformation_catalog(tc)
        #workflow.add_replica_catalog(rc) 
        
        return workflow

if __name__ == '__main__':
    parser = ArgumentParser(description="Example Workflow")
    parser.add_argument("-f", "--files", metavar="INPUT_FILES", type=str, nargs="+", help="Input Files", required=True)
    parser.add_argument("-o", "--output", metavar="OUTPUT_FILE", type=str, default="workflow.yml", help="Output file name")

    args = parser.parse_args()

    example_wf = Example_WF(args.files)
    workflow = example_wf.generate_workflow()
    workflow.write(args.output)
    workflow.graph(output="workflow.png", include_files=True, no_simplify=True, label="xform-id")
    #workflow.plan(submit=True)
    #workflow.wait()