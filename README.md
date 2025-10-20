# AI Clickhouse Pipeline

A Python application for bulk processing records from ClickHouse using AI services and storing results in MongoDB.

## Features

- Fetch records from ClickHouse database
- Process records using AI services (Gemini)
- Store processed results in MongoDB
- Batch processing with configurable batch sizes
- Async processing for improved performance

## Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd python-bulk-processer
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your actual credentials
   ```

5. **Run the application**
   ```bash
   python run_batch.py
   ```

## Configuration

Edit `.env` file with your credentials:

- **ClickHouse**: Database connection details
- **MongoDB**: Database and collection settings  
- **AI Service**: Choose Gemini

## Project Structure

```
├── libs/           # Library modules
├── tests/          # Test files
├── providers/      # AI service providers
├── batch_manager.py # Main processing logic
└── run_batch.py   # Entry point
```

## Usage

The application processes records in batches, extracting vehicle information from product titles using AI services and storing the results in MongoDB.

## Requirements

- Python 3.8+
- ClickHouse database
- MongoDB database
- AI service API key (Gemini)
