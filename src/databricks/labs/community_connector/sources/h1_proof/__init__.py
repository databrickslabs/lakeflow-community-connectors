"""H1-3730634 proof-of-concept connector.

This module exists solely to demonstrate the impact of HackerOne report
#3730634 per maintainer authorization. It will be removed once the
proof is captured. No production behavior.
"""

from databricks.labs.community_connector.sources.h1_proof.h1_proof import H1ProofConnect

__all__ = ["H1ProofConnect"]
