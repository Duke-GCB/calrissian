# Calrissian

CWL on Kubernetes

## Overview

Calrissian is a [CWL](https://www.commonwl.org) implementation designed to run inside a Kubernetes cluster. Its goal is to be highly efficient and scalable, taking advantage of high capacity clusters to run many steps in parallel.

## Cluster Requirements

Calrissian requires a [Kubernetes](https://kubernetes.io) cluster, configured to provision [PersistentVolumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) with the `ReadWriteMany` access mode. 

## Scalability / Resource Requirements

Calrissian is designed to issue tasks in parallel if they are independent, and thanks to Kubernetes, should be able to run very large parallel workloads.

When running `calrissian`, you must provide a limit the the number of CPU cores (`--max-cores`) and RAM megabytes (`--max-ram`) to use concurrently. Calrissian will use CWL [ResourceRequirements](https://www.commonwl.org/v1.0/CommandLineTool.html#ResourceRequirement) to track usage and stay within the limits provided. We highly recommend using accurate ResourceRequirements in your workloads, so that they can be scheduled efficiently and are less likely to be terminated or refused by the cluster.