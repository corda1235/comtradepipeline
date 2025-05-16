A Python-based pipeline for downloading and storing international trade data from UN Comtrade for the 27 EU countries.

## Features

- Downloads Tariffline data at 6-digit HS level
- Fetches monthly import data for EU countries
- Implements caching to minimize API calls
- Stores data in a PostgreSQL database
- Handles API rate limits and errors
- Provides detailed logging

## Requirements

- Python 3.10+
- PostgreSQL
- UN Comtrade API key

## Installation

1. Clone the repository:
   \`\`\`
   git clone <repository-url>
   cd comtrade_data_pipeline
   \`\`\`

2. Create and activate a virtual environment:
   \`\`\`
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate
   \`\`\`

3. Install dependencies:
   \`\`\`
   pip install -r requirements.txt
   \`\`\`

4. Configure the environment:
   \`\`\`
   cp .env.example .env
   # Edit .env with your API keys and database credentials
   \`\`\`

## Usage

Run the pipeline with specific parameters:

\`\`\`
python main.py --countries=DE,FR,IT --start-date=2022-01 --end-date=2022-12
\`\`\`

Or for all EU countries:

\`\`\`
python main.py --countries=all --start-date=2022-01 --end-date=2022-12
\`\`\`

## Database Schema

[Describe database schema here]

## Logging

Logs are stored in the \`logs/\` directory.

## Development

[Development guidelines here]

## License

[License information here]
