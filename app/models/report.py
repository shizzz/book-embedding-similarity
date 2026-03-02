from dataclasses import dataclass, field
from typing import Any, List, Optional

@dataclass
class Metric:
    """Одиночная метрика"""
    name: str
    value: Any
    extra: Optional[Any] = None  # Например список новых книг

@dataclass
class MetricBlock:
    """Группа метрик"""
    title: str
    metrics: List[Metric] = field(default_factory=list)

    def add(self, name: str, value: Any, extra: Optional[Any] = None):
        """Добавить метрику в блок"""
        self.metrics.append(Metric(name, value, extra))

@dataclass
class Report:
    """Отчет с блоками метрик"""
    blocks: List[MetricBlock] = field(default_factory=list)

    def create_block(self, title: str) -> MetricBlock:
        """Создать новый блок и добавить его в report"""
        block = MetricBlock(title)
        self.blocks.append(block)
        return block