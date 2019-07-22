# Calrissian

CWL on Kubernetes

[![CircleCI](https://circleci.com/gh/Duke-GCB/calrissian.svg?style=svg)](https://circleci.com/gh/Duke-GCB/calrissian)

## Overview

Calrissian is a [CWL](https://www.commonwl.org) implementation designed to run inside a Kubernetes cluster. Its goal is to be highly efficient and scalable, taking advantage of high capacity clusters to run many steps in parallel.

## Cluster Requirements

Calrissian requires a [Kubernetes](https://kubernetes.io) or [Openshift](https://www.openshift.com)/[OKD](https://www.okd.io) cluster, configured to provision [PersistentVolumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) with the `ReadWriteMany` access mode. Kubernetes installers and cloud providers don't usually include this type of storage, so it may require additional configuration.

Calrissian has been tested with NFS using the [nfs-client-provisioner](https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client) and with GlusterFS using [OKD Containerized GlusterFS](https://docs.okd.io/3.11/install_config/persistent_storage/persistent_storage_glusterfs.html). Many cloud providers have an NFS offering, which integrates easily using the nfs-client-provisioner.

## Scalability / Resource Requirements

Calrissian is designed to issue tasks in parallel if they are independent, and thanks to Kubernetes, should be able to run very large parallel workloads.

When running `calrissian`, you must provide a limit the the number of CPU cores (`--max-cores`) and RAM megabytes (`--max-ram`) to use concurrently. Calrissian will use CWL [ResourceRequirements](https://www.commonwl.org/v1.0/CommandLineTool.html#ResourceRequirement) to track usage and stay within the limits provided. We highly recommend using accurate ResourceRequirements in your workloads, so that they can be scheduled efficiently and are less likely to be terminated or refused by the cluster.

## CWL Conformance

Calrissian leverages [cwltool](https://github.com/common-workflow-language/cwltool) heavily and currently passes 122 of 128 conformance tests for CWL v1.0. Please see [conformance](conformance) for further details and processes.

To view open issues related to conformance, see the [conformance](https://github.com/Duke-GCB/calrissian/issues?q=is%3Aopen+is%3Aissue+label%3Aconformance) label on the issue tracker.

## Setup

Please see [examples](examples) for installation and setup instructions.
