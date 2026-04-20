from typing import Dict, Type, TYPE_CHECKING
from abc import ABC
import sys
from pathlib import Path

if TYPE_CHECKING:
    from normalized import Normalizer

try:
    from normalized import NormalizationError
except ImportError:
    class NormalizationError(Exception):
        pass

# assuming Normalizer base class already exists
class NormalizerRegistry:
    _registry: Dict[str, Type] = {}

    @classmethod
    def register(cls, source_type: str):
        """
        Decorator to register a normalizer.
        Usage:
            @NormalizerRegistry.register("wazuh")
            class WazuhNormalizer(...)
        """
        def decorator(normalizer_cls: Type):
            cls._registry[source_type.lower()] = normalizer_cls
            return normalizer_cls
        return decorator

    @classmethod
    def get(cls, source_type: str) -> "Normalizer":
        normalizer_cls = cls._registry.get(source_type.lower())
        if not normalizer_cls:
            raise NormalizationError(f"No normalizer found for source_type={source_type}")
        return normalizer_cls()

    @classmethod
    def supported_sources(cls):
        return list(cls._registry.keys())