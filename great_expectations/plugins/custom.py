from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.expectation import ExecutionEngine
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.expectations.expectation import \
    InvalidExpectationConfigurationError
from great_expectations.expectations.metrics import ColumnMetricProvider
from great_expectations.expectations.metrics import column_aggregate_partial
from great_expectations.expectations.metrics import column_aggregate_value
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.import_manager import sa

from typing import Dict, Optional


class ColumnCustomMax(ColumnMetricProvider):
    """MetricProvider Class for Custom Aggregate Max MetricProvider"""

    metric_name = 'column.aggregate.custom.max'

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Max Implementation"""
        return column.max()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        """SqlAlchemy Max Implementation"""
        return sa.func.max(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table, _column_name, **kwargs):
        """Spark Max Implementation"""
        types = dict(_table.dtypes)
        return F.maxcolumn()


class ExpectColumnMaxToBeBetweenCustom(ColumnExpectation):
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    metric_dependencies = ('column.aggregate.custom.max',)
    success_keys = ('min_value', 'strict_min', 'max_value', 'strict_max')

    # Default values
    default_kwarg_values = {
        'row_condition': None,
        'condition_parser': None,
        'min_value': None,
        'max_value': None,
        'strict_min': None,
        'strict_max': None,
        'mostly': 1
    }

    def validate_configuration(
            self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """
        min_val = None
        max_val = None

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # Ensuring basic configuration parameters are properly set
        try:
            assert (
                'column' in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        # Validating that Minimum and Maximum values are of the proper format and type
        if 'min_value' in configuration.kwargs:
            min_val = configuration.kwargs['min_value']

        if 'max_value' in configuration.kwargs:
            max_val = configuration.kwargs['max_value']

        # Ensuring Proper interval has been provided
        assert (min_val is not None or max_val
                is not None), 'min_value and max_value cannot both be none'
        assert min_val is None or isinstance(
            min_val, (float, int)), 'Provided min threshold must be a number'
        assert max_val is None or isinstance(
            max_val, (float, int)), 'Provided max threshold must be a number'

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set minimum and maximum value thresholds for the column max"""
        column_max = metrics.get('column.aggregate.max')

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get('min_value')
        strict_min = self.get_success_kwargs(configuration).get('strict_min')
        max_value = self.get_success_kwargs(configuration).get('max_value')
        strict_max = self.get_success_kwargs(configuration).get('strict_max')

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_max > min_value
            else:
                above_min = column_max >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_max < max_value
            else:
                below_max = column_max <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {'success': success, 'result': {'observed_value': column_max}}
