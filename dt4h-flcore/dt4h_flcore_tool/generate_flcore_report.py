"""
FLCore Federated Learning Report Generator

This script parses FLCore server logs and generates an interactive HTML report
with charts, metrics, and detailed analysis.
"""
import logging
import re
import json
import sys
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class FLCoreLogParser:
    """Parser for FLCore federated learning server logs"""
    
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.logs = []
        self.metrics = {}
        self.clients = set()
        self.rounds = 0
        self.start_time = None
        self.end_time = None
        self.total_duration = 0
        
    def parse_logs(self) -> Dict:
        """Parse the log file and extract metrics"""
        with open(self.log_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Parse individual log lines
        log_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\w+) - (\w+) - (.+)'
        
        for match in re.finditer(log_pattern, content):
            timestamp, logger, level, message = match.groups()
            self.logs.append({
                'timestamp': timestamp,
                'logger': logger,
                'level': level,
                'message': message
            })
        
        # Extract key information
        self._extract_basic_info()
        self._extract_metrics_from_logs()
        self._extract_clients()
        
        return self._compile_results()
    
    def _extract_basic_info(self):
        """Extract basic training information"""
        for log in self.logs:
            msg = log['message']
            
            # Extract number of rounds
            if 'num_rounds=' in msg:
                match = re.search(r'num_rounds=(\d+)', msg)
                if match:
                    self.rounds = int(match.group(1))
            
            # Extract start time
            if 'FL starting' in msg:
                self.start_time = log['timestamp']
            
            # Extract end time and duration
            if 'FL finished in' in msg:
                self.end_time = log['timestamp']
                match = re.search(r'FL finished in ([\d.]+)', msg)
                if match:
                    self.total_duration = float(match.group(1))
    
    def _extract_metrics_from_logs(self):
        """Extract training metrics from logs"""
        losses = []
        training_times = []
        eval_metrics = {
            'accuracy': [],
            'precision': [],
            'recall': [],
            'f1': [],
            'specificity': [],
            'balanced_accuracy': []
        }
        
        for log in self.logs:
            msg = log['message']
            
            # Extract losses
            if 'losses_distributed' in msg:
                pattern = r'\[(\d+), ([\d.]+)\]'
                for match in re.finditer(pattern, msg):
                    round_num, loss = match.groups()
                    losses.append((int(round_num), float(loss)))
            
            # Extract training times
            if 'training_time [s]' in msg:
                pattern = r'\((\d+), ([\d.]+)\)'
                for match in re.finditer(pattern, msg):
                    round_num, time_val = match.groups()
                    training_times.append((int(round_num), float(time_val)))
            # Extract evaluation metrics
            for metric in eval_metrics.keys():
                if f"'{metric}':" in msg:
                    pattern = rf"'{metric}':[^[]*\[([^]]+)\]"
                    match = re.search(pattern, msg)
                    if match:
                        values_str = match.group(1)
                        # Parse tuples like (1, 0.5265151560306549), (2, 0.5325757563114166)
                        tuple_pattern = r'\((\d+), ([\d.]+)\)'
                        for tuple_match in re.finditer(tuple_pattern, values_str):
                            round_num, value = tuple_match.groups()
                            eval_metrics[metric].append((int(round_num), float(value)))
        
        self.metrics = {
            'losses': losses[:self.rounds],
            'training_times': training_times[:self.rounds],
            'eval_metrics': eval_metrics
        }
        for metric, values in self.metrics['eval_metrics'].items():
            self.metrics['eval_metrics'][metric] = values[:self.rounds]
    
    def _extract_clients(self):
        """Extract client information"""
        for log in self.logs:
            msg = log['message']
            
            # Extract client IPs
            client_pattern = r'Client (ipv4:[\d.:]+)'
            matches = re.findall(client_pattern, msg)
            for client in matches:
                self.clients.add(client)
    
    def _compile_results(self) -> Dict:
        """Compile all extracted data into a structured format"""
        # Get final metrics (last round)
        final_metrics = {}
        for metric, values in self.metrics['eval_metrics'].items():
            if values:
                final_metrics[metric] = values[-1][1] if values else 0
        
        final_loss = self.metrics['losses'][-1][1] if self.metrics['losses'] else 0
        
        return {
            'basic_info': {
                'rounds': self.rounds,
                'clients': len(self.clients),
                'duration': self.total_duration,
                'final_loss': final_loss,
                'final_accuracy': final_metrics.get('accuracy', 0) * 100,
                'start_time': self.start_time,
                'end_time': self.end_time
            },
            'clients': list(self.clients),
            'metrics': self.metrics,
            'final_metrics': final_metrics,
            'logs': self.logs
        }


class HTMLReportGenerator:
    """Generate HTML report from parsed FLCore logs"""
    
    def __init__(self, data: Dict):
        self.data = data
    
    def generate_html(self, output_file: str = 'flwr_report.html'):
        """Generate the complete HTML report"""
        html_content = self._generate_html_structure()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logging.info(f"✅ Report generated successfully: {output_file}")
    
    def _generate_html_structure(self) -> str:
        """Generate the complete HTML structure"""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FLCore Federated Learning - Training Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    {self._generate_css()}
</head>
<body>
    <div class="container">
        {self._generate_header()}
        {self._generate_summary_cards()}
        {self._generate_progress_section()}
        {self._generate_charts_section()}
        {self._generate_client_info()}
        {self._generate_metrics_table()}
        {self._generate_logs_section()}
        {self._generate_insights()}
    </div>
    
    <footer class="bannerEU">
        <div class="container">
            <div style="text-align: center; padding: 20px; display: flex; align-items: center; justify-content: center; gap: 15px;">
                <img src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNDAiIGhlaWdodD0iMzAiIHZpZXdCb3g9IjAgMCA0MCAzMCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3Qgd2lkdGg9IjQwIiBoZWlnaHQ9IjMwIiBmaWxsPSIjMDAzMzk5Ii8+CjxjaXJjbGUgY3g9IjIwIiBjeT0iMTUiIHI9IjgiIGZpbGw9Im5vbmUiIHN0cm9rZT0iI0ZGRkZGRiIgc3Ryb2tlLXdpZHRoPSIwLjUiLz4KPGNpcmNsZSBjeD0iMjAiIGN5PSI3IiByPSIxIiBmaWxsPSIjRkZGRkZGIi8+CjxjaXJjbGUgY3g9IjI0LjMzIiBjeT0iOC41IiByPSIxIiBmaWxsPSIjRkZGRkZGIi8+CjxjaXJjbGUgY3g9IjI3IiBjeT0iMTIiIHI9IjEiIGZpbGw9IiNGRkZGRkYiLz4KPGNpcmNsZSBjeD0iMjcuNSIgY3k9IjE2IiByPSIxIiBmaWxsPSIjRkZGRkZGIi8+CjxjaXJjbGUgY3g9IjI2IiBjeT0iMjAiIHI9IjEiIGZpbGw9IiNGRkZGRkYiLz4KPGNpcmNsZSBjeD0iMjMiIGN5PSIyMiIgcj0iMSIgZmlsbD0iI0ZGRkZGRiIvPgo8Y2lyY2xlIGN4PSIyMCIgY3k9IjIzIiByPSIxIiBmaWxsPSIjRkZGRkZGIi8+CjxjaXJjbGUgY3g9IjE3IiBjeT0iMjIiIHI9IjEiIGZpbGw9IiNGRkZGRkYiLz4KPGNpcmNsZSBjeD0iMTQiIGN5PSIyMCIgcj0iMSIgZmlsbD0iI0ZGRkZGRiIvPgo8Y2lyY2xlIGN4PSIxMi41IiBjeT0iMTYiIHI9IjEiIGZpbGw9IiNGRkZGRkYiLz4KPGNpcmNsZSBjeD0iMTMiIGN5PSIxMiIgcj0iMSIgZmlsbD0iI0ZGRkZGRiIvPgo8Y2lyY2xlIGN4PSIxNS42NyIgY3k9IjguNSIgcj0iMSIgZmlsbD0iI0ZGRkZGRiIvPgo8L3N2Zz4K" alt="European Union Flag" style="height: 24px; width: auto;">
                <div style="text-align: left;">
                    <p style="margin: 5px 0 0 0; font-size: 12px; opacity: 0.8;">
                        This project has received funding from the European Union's Horizon 2020 research and innovation programme under grant agreement No 101016496.
                    </p>
                </div>
            </div>
        </div>
    </footer>
    
    {self._generate_javascript()}
</body>
</html>"""
    
    def _generate_css(self) -> str:
        """Generate CSS styles matching DataTools4Heart BSC branding"""
        return """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        
        :root {
            --dt4h-primary: #ae0d1b;
            --dt4h-secondary: #8b0a15;
            --dt4h-accent: #d42434;
            --dt4h-success: #28a745;
            --dt4h-warning: #ffc107;
            --dt4h-danger: #dc3545;
            --dt4h-light: #f8f9fa;
            --dt4h-dark: #212529;
            --dt4h-gray-100: #f1f3f4;
            --dt4h-gray-200: #e9ecef;
            --dt4h-gray-300: #dee2e6;
            --dt4h-gray-600: #6c757d;
            --dt4h-gray-700: #495057;
            --dt4h-gray-800: #343a40;
        }
        
        * {
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 0;
            background: #ffffff;
            color: var(--dt4h-dark);
            line-height: 1.6;
            min-height: 100vh;
        }
        
        .header {
            background: linear-gradient(rgba(174, 13, 27, 0.8), rgba(139, 10, 21, 0.8)), url('https://www.datatools4heart.eu/wp-content/uploads/2023/03/Bg-1.png');
            background-size: cover;
            background-position: center center;
            background-repeat: no-repeat;
            color: white;
            padding: 40px 20px;
            text-align: center;
            margin-bottom: 40px;
            box-shadow: 0 8px 32px rgba(174, 13, 27, 0.3);
            position: relative;
            overflow: hidden;
        }
        
        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.1);
            z-index: 0;
        }
        
        .header h1 {
            margin: 0;
            font-size: 3em;
            font-weight: 700;
            position: relative;
            z-index: 1;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header .subtitle {
            margin: 15px 0 0 0;
            opacity: 0.95;
            font-size: 1.2em;
            font-weight: 400;
            position: relative;
            z-index: 1;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 0 20px;
        }
        
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 25px;
            margin-bottom: 40px;
        }
        
        .summary-card {
            background: white;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 8px 25px rgba(174, 13, 27, 0.1);
            text-align: center;
            border-left: 5px solid var(--dt4h-primary);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        
        .summary-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, var(--dt4h-primary), var(--dt4h-accent));
        }
        
        .summary-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 12px 35px rgba(174, 13, 27, 0.2);
        }
        
        .summary-card h3 {
            margin: 0 0 15px 0;
            color: var(--dt4h-gray-700);
            font-size: 0.95em;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            font-weight: 600;
        }
        
        .summary-card .value {
            font-size: 2.5em;
            font-weight: 700;
            color: var(--dt4h-primary);
            margin-bottom: 5px;
        }
        
        .section {
            background: white;
            margin: 40px 0;
            padding: 35px;
            border-radius: 12px;
            box-shadow: 0 8px 25px rgba(174, 13, 27, 0.08);
            border: 1px solid var(--dt4h-gray-200);
        }
        
        .section h2 {
            color: var(--dt4h-primary);
            border-bottom: 3px solid var(--dt4h-gray-200);
            padding-bottom: 15px;
            margin-bottom: 30px;
            font-size: 1.8em;
            font-weight: 600;
            position: relative;
        }
        
        .section h2::after {
            content: '';
            position: absolute;
            bottom: -3px;
            left: 0;
            width: 60px;
            height: 3px;
            background: linear-gradient(90deg, var(--dt4h-primary), var(--dt4h-accent));
        }
        
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
            gap: 35px;
            margin: 35px 0;
        }
        
        .chart-container {
            background: white;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 8px 25px rgba(174, 13, 27, 0.08);
            border: 1px solid var(--dt4h-gray-200);
            transition: all 0.3s ease;
        }
        
        .chart-container:hover {
            box-shadow: 0 12px 35px rgba(174, 13, 27, 0.15);
        }
        
        .chart-container h3 {
            color: var(--dt4h-primary);
            margin-bottom: 25px;
            text-align: center;
            font-weight: 600;
            font-size: 1.2em;
        }
        
        .client-info {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 25px;
        }
        
        .client-card {
            background: var(--dt4h-light);
            padding: 25px;
            border-radius: 10px;
            border: 2px solid var(--dt4h-gray-200);
            transition: all 0.3s ease;
        }
        
        .client-card:hover {
            border-color: var(--dt4h-accent);
            background: white;
        }
        
        .client-card h4 {
            color: var(--dt4h-primary);
            margin: 0 0 15px 0;
            font-size: 1.1em;
            font-weight: 600;
        }
        
        .metric-table {
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            font-size: 0.95em;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 4px 15px rgba(174, 13, 27, 0.1);
        }
        
        .metric-table th, .metric-table td {
            padding: 15px 12px;
            text-align: left;
            border-bottom: 1px solid var(--dt4h-gray-200);
        }
        
        .metric-table th {
            background: linear-gradient(135deg, var(--dt4h-primary), var(--dt4h-secondary));
            color: white;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }
        
        .metric-table tr:hover {
            background-color: rgba(174, 13, 27, 0.05);
        }
        
        .metric-table tr:nth-child(even) {
            background-color: rgba(0, 180, 216, 0.02);
        }
        
        .log-section {
            max-height: 450px;
            overflow-y: auto;
            background: linear-gradient(135deg, var(--dt4h-gray-800) 0%, #2c3e50 100%);
            color: #ecf0f1;
            padding: 25px;
            border-radius: 10px;
            font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
            font-size: 0.85em;
            line-height: 1.5;
            border: 2px solid var(--dt4h-gray-300);
        }
        
        .log-entry {
            margin: 3px 0;
            padding: 3px 0;
            border-left: 3px solid transparent;
            padding-left: 8px;
        }
        
        .log-debug { 
            color: #74c0fc; 
            border-left-color: #74c0fc;
        }
        .log-info { 
            color: #51cf66; 
            border-left-color: #51cf66;
        }
        .log-warning { 
            color: #ffd43b; 
            border-left-color: #ffd43b;
        }
        .log-error { 
            color: #ff6b6b; 
            border-left-color: #ff6b6b;
        }
        
        .timestamp {
            color: #adb5bd;
            font-weight: 600;
        }
        
        .badge {
            display: inline-block;
            padding: 6px 12px;
            background: var(--dt4h-primary);
            color: white;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: 500;
            margin: 3px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-success {
            background: linear-gradient(135deg, var(--dt4h-success) 0%, #20c997 100%);
        }
        
        .progress-bar {
            width: 100%;
            height: 25px;
            background: var(--dt4h-gray-200);
            border-radius: 15px;
            overflow: hidden;
            margin: 15px 0;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--dt4h-primary) 0%, var(--dt4h-accent) 100%);
            width: 100%;
            transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
        }
        
        .progress-fill::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
            animation: shimmer 2s infinite;
        }
        
        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }
        
        /* EU Banner styles matching dt4h.css */
        .bannerEU {
            background-color: var(--dt4h-primary);
            color: #fff;
            margin-top: 50px;
        }
        
        /* Responsive design */
        @media (max-width: 768px) {
            .header h1 { font-size: 2.2em; }
            .header .subtitle { font-size: 1.1em; }
            .section { padding: 25px 20px; }
            .charts-grid { grid-template-columns: 1fr; }
            .summary-grid { grid-template-columns: 1fr; }
            
            /* Footer responsive styling */
            .bannerEU .container > div {
                flex-direction: column !important;
                text-align: center !important;
            }
            .bannerEU .container > div > div {
                text-align: center !important;
            }
        }
    </style>"""
    
    def _generate_header(self) -> str:
        """Generate header section"""
        current_date = datetime.now().strftime("%B %d, %Y")
        return f"""
        <div class="header">
            <h1><img src="https://fl.datatools4heart.bsc.es/assets/layouts/layout/img/logo.png" alt="DataTools4Heart Logo" style="height: 50px;"> FLCore Federated Learning Report</h1>
            <div class="subtitle">Training Session - {current_date} | FLCore Analytics Dashboard</div>
        </div>"""
    
    def _generate_summary_cards(self) -> str:
        """Generate summary cards section"""
        info = self.data['basic_info']
        return f"""
        <div class="summary-grid">
            <div class="summary-card">
                <h3>Total Rounds</h3>
                <div class="value">{info['rounds']}</div>
            </div>
            <div class="summary-card">
                <h3>Active Clients</h3>
                <div class="value">{info['clients']}</div>
            </div>
            <div class="summary-card">
                <h3>Training Duration</h3>
                <div class="value">{info['duration']:.2f}s</div>
            </div>
            <div class="summary-card">
                <h3>Final Loss</h3>
                <div class="value">{info['final_loss']:.4f}</div>
            </div>
            <div class="summary-card">
                <h3>Final Accuracy</h3>
                <div class="value">{info['final_accuracy']:.2f}%</div>
            </div>
        </div>"""
    
    def _generate_progress_section(self) -> str:
        """Generate progress section"""
        return """
        <div class="section">
            <h2>📊 Training Progress</h2>
            <div class="progress-bar">
                <div class="progress-fill"></div>
            </div>
            <p><span class="badge status-success">COMPLETED</span> Federated learning completed successfully</p>
        </div>"""
    
    def _generate_charts_section(self) -> str:
        """Generate charts section"""
        return """
        <div class="charts-grid">
            <div class="chart-container">
                <h3>Loss Over Rounds</h3>
                <canvas id="lossChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>Accuracy Over Rounds</h3>
                <canvas id="accuracyChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>Training Time per Round</h3>
                <canvas id="timeChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>Performance Metrics (Final Round)</h3>
                <canvas id="metricsChart"></canvas>
            </div>
        </div>"""
    
    def _generate_client_info(self) -> str:
        """Generate client information section"""
        clients_html = ""
        for i, client in enumerate(self.data['clients'], 1):
            client_type = "Docker Network Client" if "172.17.0.1" in client else "External Network Client"
            clients_html += f"""
                <div class="client-card">
                    <h4>Client {i}: {client.replace('ipv4:', '')}</h4>
                    <p><strong>Type:</strong> {client_type}</p>
                    <p><strong>Samples per Round:</strong> 660</p>
                    <p><strong>Status:</strong> <span class="badge status-success">Active</span></p>
                </div>"""
        
        return f"""
        <div class="section">
            <h2>🏥 Client Information</h2>
            <div class="client-info">
                {clients_html}
            </div>
        </div>"""
    
    def _generate_metrics_table(self) -> str:
        """Generate metrics table"""
        rows = ""
        metrics = self.data['metrics']
        
        # Combine data by rounds
        rounds_data = {}
        
        for round_num, loss in metrics['losses']:
            rounds_data[round_num] = {'loss': loss}
        
        for metric, values in metrics['eval_metrics'].items():
            for round_num, value in values:
                if round_num not in rounds_data:
                    rounds_data[round_num] = {}
                rounds_data[round_num][metric] = value
        
        for round_num, training_time in metrics['training_times']:
            if round_num in rounds_data:
                rounds_data[round_num]['training_time'] = training_time
        
        for round_num in sorted(rounds_data.keys()):
            data = rounds_data[round_num]
            rows += f"""
                <tr>
                    <td>{round_num}</td>
                    <td>{data.get('loss', 0):.4f}</td>
                    <td>{data.get('accuracy', 0)*100:.2f}%</td>
                    <td>{data.get('precision', 0)*100:.2f}%</td>
                    <td>{data.get('recall', 0)*100:.2f}%</td>
                    <td>{data.get('f1', 0)*100:.2f}%</td>
                    <td>{data.get('training_time', 0):.2f}</td>
                </tr>"""
        
        return f"""
        <div class="section">
            <h2>📈 Detailed Metrics by Round</h2>
            <table class="metric-table">
                <thead>
                    <tr>
                        <th>Round</th>
                        <th>Loss</th>
                        <th>Accuracy</th>
                        <th>Precision</th>
                        <th>Recall</th>
                        <th>F1 Score</th>
                        <th>Training Time (s)</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>"""
    
    def _generate_logs_section(self) -> str:
        """Generate logs section"""
        log_entries = ""
        
        # Show first 10 and last 5 log entries
        selected_logs = self.data['logs'][:10] + self.data['logs'][-5:]
        
        for log in selected_logs:
            level_class = f"log-{log['level'].lower()}"
            log_entries += f"""
                <div class="log-entry {level_class}">
                    <span class="timestamp">{log['timestamp']}</span> - {log['logger']} - {log['level']} - {log['message']}
                </div>"""
        
        return f"""
        <div class="section">
            <h2>📋 Server Log Details (Sample)</h2>
            <div class="log-section">
                {log_entries}
            </div>
        </div>"""
    
    def _generate_insights(self) -> str:
        """Generate insights section"""
        info = self.data['basic_info']
        return f"""
        <div class="section">
            <h2>Key Insights</h2>
            <ul style="font-size: 1.1em; line-height: 1.8;">
                <li><strong>Training Stability:</strong> Training completed with {info['rounds']} rounds</li>
                <li><strong>Client Participation:</strong> {info['clients']} clients participated in the training</li>
                <li><strong>Performance Metrics:</strong> Final accuracy reached {info['final_accuracy']:.2f}%</li>
                <li><strong>Training Efficiency:</strong> Total training completed in {info['duration']:.2f} seconds</li>
                <li><strong>Final Loss:</strong> Training converged to loss value of {info['final_loss']:.4f}</li>
            </ul>
        </div>"""
    
    def _generate_javascript(self) -> str:
        """Generate JavaScript for charts"""
        # Prepare data for JavaScript with safety checks
        metrics = self.data.get('metrics', {})
        
        # Extract losses with fallback
        losses_data = metrics.get('losses', [])
        losses = [loss for _, loss in losses_data] if losses_data else [0.0]
        
        # Extract accuracies with fallback
        eval_metrics = metrics.get('eval_metrics', {})
        accuracy_data = eval_metrics.get('accuracy', [])
        accuracies = [acc*100 for _, acc in accuracy_data] if accuracy_data else [0.0]
        
        # Extract training times with fallback
        training_data = metrics.get('training_times', [])
        training_times = [time for _, time in training_data] if training_data else [0.0]
        
        # Ensure all arrays have the same length
        max_length = max(len(losses), len(accuracies), len(training_times), 1)
        
        # Pad arrays to same length if needed
        while len(losses) < max_length:
            losses.append(losses[-1] if losses else 0.0)
        while len(accuracies) < max_length:
            accuracies.append(accuracies[-1] if accuracies else 0.0)
        while len(training_times) < max_length:
            training_times.append(training_times[-1] if training_times else 0.0)
        
        final_metrics = self.data.get('final_metrics', {})
        metrics_values = [
            final_metrics.get('accuracy', 0) * 100,
            final_metrics.get('precision', 0) * 100,
            final_metrics.get('recall', 0) * 100,
            final_metrics.get('f1', 0) * 100,
            final_metrics.get('specificity', 0) * 100,
            final_metrics.get('balanced_accuracy', 0) * 100
        ]
        
        rounds_labels = [f'Round {i+1}' for i in range(max_length)]
        
        return f"""
    <script>
        // DataTools4Heart color palette
        const dt4hColors = {{
            primary: '#ae0d1b',
            secondary: '#8b0a15',
            accent: '#d42434',
            success: '#28a745',
            warning: '#ffc107',
            danger: '#dc3545'
        }};

        // Chart.js default configuration
        Chart.defaults.font.family = 'Inter, sans-serif';
        Chart.defaults.font.size = 12;
        Chart.defaults.color = '#495057';

        // Loss Chart
        const lossCtx = document.getElementById('lossChart').getContext('2d');
        new Chart(lossCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(rounds_labels)},
                datasets: [{{
                    label: 'Training Loss',
                    data: {json.dumps(losses)},
                    borderColor: dt4hColors.primary,
                    backgroundColor: dt4hColors.primary + '20',
                    tension: 0.4,
                    fill: true,
                    borderWidth: 3,
                    pointBackgroundColor: dt4hColors.primary,
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    pointRadius: 6,
                    pointHoverRadius: 8
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        labels: {{
                            usePointStyle: true,
                            font: {{
                                weight: 600
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: false,
                        grid: {{
                            color: '#e9ecef'
                        }},
                        ticks: {{
                            color: '#6c757d'
                        }}
                    }},
                    x: {{
                        grid: {{
                            color: '#e9ecef'
                        }},
                        ticks: {{
                            color: '#6c757d'
                        }}
                    }}
                }}
            }}
        }});

        // Accuracy Chart
        const accuracyCtx = document.getElementById('accuracyChart').getContext('2d');
        new Chart(accuracyCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(rounds_labels)},
                datasets: [{{
                    label: 'Accuracy (%)',
                    data: {json.dumps(accuracies)},
                    borderColor: dt4hColors.success,
                    backgroundColor: dt4hColors.success + '20',
                    tension: 0.4,
                    fill: true,
                    borderWidth: 3,
                    pointBackgroundColor: dt4hColors.success,
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    pointRadius: 6,
                    pointHoverRadius: 8
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        labels: {{
                            usePointStyle: true,
                            font: {{
                                weight: 600
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: false,
                        grid: {{
                            color: '#e9ecef'
                        }},
                        ticks: {{
                            color: '#6c757d'
                        }}
                    }},
                    x: {{
                        grid: {{
                            color: '#e9ecef'
                        }},
                        ticks: {{
                            color: '#6c757d'
                        }}
                    }}
                }}
            }}
        }});

        // Training Time Chart
        const timeCtx = document.getElementById('timeChart').getContext('2d');
        new Chart(timeCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(rounds_labels)},
                datasets: [{{
                    label: 'Training Time (s)',
                    data: {json.dumps(training_times)},
                    backgroundColor: dt4hColors.accent + 'CC',
                    borderColor: dt4hColors.accent,
                    borderWidth: 2,
                    borderRadius: 6,
                    borderSkipped: false,
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        labels: {{
                            usePointStyle: true,
                            font: {{
                                weight: 600
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        grid: {{
                            color: '#e9ecef'
                        }},
                        ticks: {{
                            color: '#6c757d'
                        }}
                    }},
                    x: {{
                        grid: {{
                            display: false
                        }},
                        ticks: {{
                            color: '#6c757d'
                        }}
                    }}
                }}
            }}
        }});

        // Performance Metrics Chart (Final Round)
        const metricsCtx = document.getElementById('metricsChart').getContext('2d');
        new Chart(metricsCtx, {{
            type: 'radar',
            data: {{
                labels: ['Accuracy', 'Precision', 'Recall', 'F1 Score', 'Specificity', 'Balanced Accuracy'],
                datasets: [{{
                    label: 'Performance Metrics (%)',
                    data: {json.dumps(metrics_values)},
                    borderColor: dt4hColors.primary,
                    backgroundColor: dt4hColors.primary + '30',
                    pointBackgroundColor: dt4hColors.primary,
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 3,
                    pointRadius: 6,
                    pointHoverRadius: 8,
                    borderWidth: 3
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        labels: {{
                            usePointStyle: true,
                            font: {{
                                weight: 600
                            }}
                        }}
                    }}
                }},
                scales: {{
                    r: {{
                        beginAtZero: true,
                        max: 100,
                        grid: {{
                            color: '#e9ecef'
                        }},
                        angleLines: {{
                            color: '#e9ecef'
                        }},
                        pointLabels: {{
                            color: '#6c757d',
                            font: {{
                                size: 11,
                                weight: 600
                            }}
                        }},
                        ticks: {{
                            color: '#6c757d',
                            backdropColor: 'transparent'
                        }}
                    }}
                }}
            }}
        }});
    </script>"""
