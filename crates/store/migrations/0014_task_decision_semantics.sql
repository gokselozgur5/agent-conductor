-- Decisions live in the existing authoritative task aggregate and atomic audit.
-- This semantic migration deliberately changes no table. Recording its version
-- makes older binaries refuse startup instead of decoding away decision fields
-- and completing work without the new unresolved-decision gate.
SELECT 1;
