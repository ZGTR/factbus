# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's "Report a vulnerability"
(Security tab of this repository). Do not open a public issue. Expect an acknowledgement
within 5 working days.

## Scope

In scope: the `factbus` package and the modules under `iac/modules`. Typical concerns are a
consumer applying an event twice, `once` repeating a side effect, a producer publishing
under another service's `source`, or an IAM policy broader than documented.

## What factbus never does

- It reads no secret. AWS credentials come from the runtime (Lambda role, environment).
- It does not log event `data`. It does log the message of an exception your effect
  raises, with the event id, so keep personal data out of exception messages. Metrics
  carry counts and outcomes only.
- The `producer` module allows `events:PutEvents` only with the producer's own `source`.
