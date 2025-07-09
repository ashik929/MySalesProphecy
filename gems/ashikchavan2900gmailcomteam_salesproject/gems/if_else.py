from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SString
from prophecy.cb.ui.uispec import *


class IF_else_dynamic(ComponentSpec):
    name: str = "IF_else_dynamic"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class IF_else_dynamicProperties(ComponentProperties):
        column_name: SString = SString("price")
        operator: SString = SString(">")
        compare_value: SString = SString("100")
        true_value: SString = SString("'High'")
        false_value: SString = SString("'Low'")
        output_column: SString = SString("price_flag")

    def dialog(self) -> Dialog:
        return Dialog("IF_else_dynamic", [
            TextBox("column_name", "Column to check (e.g., price)"),
            TextBox("operator", "Operator (e.g., >, =, <)"),
            TextBox("compare_value", "Compare with (e.g., 100 or 'IN')"),
            TextBox("true_value", "Value if True (e.g., 'High')"),
            TextBox("false_value", "Value if False (e.g., 'Low')"),
            TextBox("output_column", "Output Column (e.g., price_flag)")
        ])

    def validate(self, context: WorkflowContext, component: Component[IF_else_dynamicProperties]) -> List[Diagnostic]:
        return []

    def onChange(self, context: WorkflowContext,
                 oldState: Component[IF_else_dynamicProperties],
                 newState: Component[IF_else_dynamicProperties]) -> Component[IF_else_dynamicProperties]:
        return newState

    class IF_else_dynamicCode(ComponentCode):
        def __init__(self, newProps):
            self.props: IF_else_dynamic.IF_else_dynamicProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            cond_expr = f"{self.props.column_name.value} {self.props.operator.value} {self.props.compare_value.value}"
            true_val = expr(self.props.true_value.value)
            false_val = expr(self.props.false_value.value)
            output_col = self.props.output_column.value

            return in0.withColumn(output_col, when(expr(cond_expr), true_val).otherwise(false_val))
