def event_id_of(record):  # GATE_MUST_FLAG_THIS clause 5: a raw SQS record in domain
    return record["body"]
