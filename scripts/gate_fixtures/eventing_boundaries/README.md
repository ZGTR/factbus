Fixtures for `check_eventing_boundaries.py`. Each directory is a stand-in `src/` root holding a
`factbus` package. `conforming/` must produce no finding. Each `offending/<name>/` must
produce findings for exactly one clause, named by its prefix (`c1_` = clause 1). These files are
never imported; the gate reads them with `ast`.
