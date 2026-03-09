## pytesting for report_writeer.py
import pytest
from CafeSalesAnalysis import report_writer

# 1. test that a report file is created
# 2. test that report content is written
# 3. test total record count
# 4. test aggregations appear in the report
# 5. test error reporting
# 6. test empty dataset handling
# 7. test timestamps are included
# 8. test csv report writing
# 9. test report formatting
# 10. test invalid file path handling