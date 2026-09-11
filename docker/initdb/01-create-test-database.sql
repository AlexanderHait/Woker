-- The test suite drops and recreates its schema on every run, so it gets its own
-- database. The name must end in `_test`: tests/conftest.py refuses to reset anything
-- else, which is what stops a stray INTAKE_DATABASE_URL from wiping real data.
CREATE DATABASE intake_test OWNER intake;
