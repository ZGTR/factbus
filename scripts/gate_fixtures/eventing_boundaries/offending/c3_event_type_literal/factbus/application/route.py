def is_delivery(envelope):
    return envelope["type"] == "comms.delivery.requested"  # GATE_MUST_FLAG_THIS clause 3
