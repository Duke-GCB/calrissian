# calrissian

CWL on Kubernetes

[![CircleCI](https://circleci.com/gh/Duke-GCB/calrissian.svg?style=svg)](https://circleci.com/gh/Duke-GCB/calrissian)

## Overview

Calrissian is a CWL implementation designed to run individual steps as Jobs in a kubernetes cluster

It is in development and includes a simple workflow (revsort) to reverse and sort the contents of text files.

## Preparing Openshift

The `openshift/` directory contains YAML files that demonstrate the basic functionality. To configure Openshift, do the following:

1. Create a project in openshift (e.g. `calrissian`)

        oc new-project calrissian

2. Create a role in your project to allow managing `Job`s:

        oc create role job-manager-role --verb=get,list,watch,create,delete --resource=jobs

3. Give your project's default service account access to this role. This allows the calrissian Job to create Jobs for each step.

        oc create rolebinding job-manager-default-binding --role=job-manager-role --serviceaccount=calrissian:default

4. Create the BuildConfig - this allows Openshift to build the source code into a Docker image and starts a build automatically. If you wish to build a different branch or repo, edit this file.

        oc create -f openshift/BuildConfig.yaml

5. Create the VolumeClaims - these are the storage locations that will be shared between Jobs, and must support read-write many access.

        oc create -f openshift/VolumeClaims.yaml

## Running the example

With the VolumeClaims and Build processes in place, you can run the example with 2 jobs

1. Stage the input data and workflow onto the `input-data` volume with the StageDataJob.

        oc create -f openshift/StageDataJob-revsort.yaml

2. Run the workflow engine with the CalrissianJob

        oc create -f openshift/CalrissianJob-revsort.yaml

3. Job pods can be monitored from the openshift web interface or via command-line

        oc log -f job/calrissian-revsort-array

## Notes

1. Jobs cannot use ImageStreams, they must use standard docker registry URLs. Since these URLs include the namespace/project, you'll likely need to tweak the urls in the Job YAML if your project is not named `calrissian`.
2. Calrissian will delete completed jobs for individual steps, but you may want to delete the `calrissian-revsort-array` job manually after running it.
