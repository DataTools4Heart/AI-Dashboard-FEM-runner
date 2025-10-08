import sys
from generate_flcore_report import FLCoreLogParser, HTMLReportGenerator

input_log_file, output_report_file = "BSC_log_server.txt", "flcore_report.html"
if len(sys.argv) > 1:
    input_log_file = sys.argv[1]
if len(sys.argv) > 2:
    output_report_file = sys.argv[2]    

Flwr_log_file = input_log_file
parser = FLCoreLogParser(Flwr_log_file)
data = parser.parse_logs()
report_file = output_report_file
generator = HTMLReportGenerator(data)
generator.generate_html(report_file)
