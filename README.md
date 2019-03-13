# calrissian

CWL on Kubernetes

[![CircleCI](https://circleci.com/gh/Duke-GCB/calrissian.svg?style=svg)](https://circleci.com/gh/Duke-GCB/calrissian)

## Overview

Calrissian is a [CWL](https://www.commonwl.org) implementation designed to run individual steps as Pods in a kubernetes cluster

It is in development and includes a simple workflow (revsort [single](input-data/revsort-single.cwl) / [array](input-data/revsort-array.cwl)) to reverse and sort the contents of text files.

## Preparing Openshift

The `openshift/` directory contains YAML files that demonstrate the basic functionality. To configure Openshift, do the following:

1. Create a project in openshift (e.g. `calrissian-demo-project`). Note that namespaces must be unique, so if `calrissian-demo-project` is already in use on the cluster, you will need to choose a different name.

        oc new-project calrissian-demo-project

2. Create a role in your project to allow managing `Pod`s:

        oc create role pod-manager-role --verb=create,delete,list,watch --resource=pods
        oc create role log-reader-role --verb=get,list --resource=pods/log

3. Bind your project's default service account access to these roles. Calrissian running inside the cluster will use this service account to make Kubernetes API calls.

        oc create rolebinding pod-manager-default-binding --role=pod-manager-role --serviceaccount=calrissian-demo-project:default
        oc create rolebinding log-reader-default-binding --role=log-reader-role --serviceaccount=calrissian-demo-project:default

4. Create the BuildConfig - this allows Openshift to build the source code into a Docker image and starts a build automatically. If you wish to build a different branch or repo, edit this file.

        oc create -f openshift/BuildConfig.yaml

5. Create the VolumeClaims - these are the storage locations that will be shared between pods, and must support read-write many access.

        oc create -f openshift/VolumeClaims.yaml

## Running the example

With the VolumeClaims and Build processes in place, you can run the example with 2 jobs. These jobs use the docker image built by the BuildConfig, so they cannot be run until the build completes.

1. Stage the input data and workflow onto the `input-data` volume with the StageInputDataJob.

        oc create -f openshift/StageInputDataJob.yaml

2. Run the workflow engine with the CalrissianJob

        oc create -f openshift/CalrissianJob-revsort.yaml

3. Job pods can be monitored from the openshift web interface or via command-line

        oc logs -f job/calrissian-revsort-array

## Notes

1. Calrissian will delete completed jobs for individual steps, but you may want to delete the `calrissian-revsort-array` job manually after running it.
