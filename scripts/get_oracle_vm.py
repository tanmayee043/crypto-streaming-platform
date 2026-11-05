"""Asks Oracle for the free ARM VM until a slot opens (Always Free capacity is often full).

Run by .github/workflows/get-vm.yml every 30 minutes. Safe to run repeatedly: it never creates
a second VM, and only ever asks for sizes inside the Always Free limit. Credentials come from
environment variables, never from the repo. Temporary: delete this and the workflow once the
VM exists.
"""

import os
import sys
import time

import oci

NAME = "crypto-streaming"
SHAPE = "VM.Standard.A1.Flex"
# largest first; both are inside the Always Free allowance of 2 OCPUs and 12 GB
SIZES = [(2, 12), (1, 6)]
PAUSE_BETWEEN_ATTEMPTS_SECONDS = 5
GONE = {"TERMINATED", "TERMINATING"}

# what the job should do next
CREATED, ALREADY_EXISTS, NO_CAPACITY, THROTTLED = "created", "exists", "no-capacity", "throttled"


class SetupProblem(Exception):
    """Something only a person can fix, so the job should fail and email you."""


def live_arm_instances(compute, compartment):
    instances = oci.pagination.list_call_get_all_results(compute.list_instances, compartment).data
    return [i for i in instances if i.shape == SHAPE and i.lifecycle_state not in GONE]


def public_subnet(network, compartment):
    subnets = oci.pagination.list_call_get_all_results(network.list_subnets, compartment).data
    usable = [s for s in subnets if s.lifecycle_state == "AVAILABLE" and not s.prohibit_public_ip_on_vnic]
    if not usable:
        raise SetupProblem("No public subnet found. Create one with the VCN wizard in the Oracle console.")
    return usable[0]


def ubuntu_image(compute, compartment):
    images = compute.list_images(compartment, operating_system="Canonical Ubuntu", operating_system_version="24.04",
                                 shape=SHAPE, sort_by="TIMECREATED", sort_order="DESC").data
    if not images:
        raise SetupProblem(f"No Ubuntu 24.04 image found for {SHAPE}.")
    return images[0]


def launch_details(compartment, domain, subnet, image, ssh_key, ocpus, memory):
    return oci.core.models.LaunchInstanceDetails(
        compartment_id=compartment,
        availability_domain=domain,
        display_name=NAME,
        shape=SHAPE,
        shape_config=oci.core.models.LaunchInstanceShapeConfigDetails(ocpus=ocpus, memory_in_gbs=memory),
        source_details=oci.core.models.InstanceSourceViaImageDetails(image_id=image.id),
        create_vnic_details=oci.core.models.CreateVnicDetails(subnet_id=subnet.id, assign_public_ip=True),
        metadata={"ssh_authorized_keys": ssh_key},
    )


def try_to_get_vm(compute, identity, network, compartment, ssh_key, sleep=time.sleep):
    """Returns CREATED, ALREADY_EXISTS, NO_CAPACITY or THROTTLED."""
    if live_arm_instances(compute, compartment):
        print("An ARM VM already exists, so nothing was requested.")
        return ALREADY_EXISTS

    subnet = public_subnet(network, compartment)
    image = ubuntu_image(compute, compartment)
    domains = identity.list_availability_domains(compartment).data

    for ocpus, memory in SIZES:
        for domain in domains:
            try:
                compute.launch_instance(
                    launch_details(compartment, domain.name, subnet, image, ssh_key, ocpus, memory),
                    retry_strategy=oci.retry.NoneRetryStrategy(),
                )
            except oci.exceptions.ServiceError as e:
                if e.status == 429:
                    print("Oracle asked us to slow down; stopping until the next scheduled run.")
                    return THROTTLED
                if "capacity" in str(e.message).lower():
                    print(f"No capacity: {ocpus} OCPU / {memory} GB in {domain.name}")
                    sleep(PAUSE_BETWEEN_ATTEMPTS_SECONDS)
                    continue
                raise  # anything else needs a person: fail the job so GitHub emails you
            print(f"Requested the VM: {ocpus} OCPU / {memory} GB in {domain.name}")
            return CREATED
    return NO_CAPACITY


def main():
    config = {
        "tenancy": os.environ["OCI_TENANCY"],
        "user": os.environ["OCI_USER"],
        "fingerprint": os.environ["OCI_FINGERPRINT"],
        "key_content": os.environ["OCI_PRIVATE_KEY"],
        "region": os.environ["OCI_REGION"],
    }
    oci.config.validate_config(config)
    result = try_to_get_vm(
        oci.core.ComputeClient(config),
        oci.identity.IdentityClient(config),
        oci.core.VirtualNetworkClient(config),
        config["tenancy"],
        os.environ["SSH_PUBLIC_KEY"],
    )
    output = os.getenv("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as f:
            f.write(f"result={result}\n")
    print(f"Result: {result}")


if __name__ == "__main__":
    try:
        main()
    except SetupProblem as e:
        sys.exit(str(e))
