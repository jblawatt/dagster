from dagster import job, op


# fmt: off
# start_k8s_config_op
@op(
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    }
)
def my_op(context):
    context.log.info("running")
# end_k8s_config_op

# start_k8s_config_job
@job(
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "250m", "memory": "64Mi"},
                    "limits": {"cpu": "500m", "memory": "2560Mi"},
                },
                "volumeMounts": [
                    {"name": "volume1", "mountPath": "foo/bar", "subPath": "file.txt"}
                ],
            },
            "pod_template_spec_metadata": {
                "annotations": {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}
            },
            "pod_spec_config": {
                "volumes": [{"name": "volume1", "secret": {"secretName": "volume_secret_name"}}],
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "beta.kubernetes.io/os",
                                            "operator": "In",
                                            "values": ["windows", "linux"],
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
            },
        },
    },
)
def my_job():
    my_op()
# end_k8s_config_job
# fmt: on
