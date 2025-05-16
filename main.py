import sys
import argparse
from loguru import logger
from src.pipeline import ComtradePipeline
from src.utils.config_loader import load_config


def setup_argparse():
    \"\"\"Set up command line arguments.\"\"\"
    parser = argparse.ArgumentParser(description='Download and store UN Comtrade data for EU countries.')
    parser.add_argument('--countries', type=str, help='Comma-separated list of EU country codes or \"all\" for all 27 EU countries')
    parser.add_argument('--start-date', type=str, required=True, help='Start date in YYYY-MM format')
    parser.add_argument('--end-date', type=str, required=True, help='End date in YYYY-MM format')
    parser.add_argument('--log-level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set the logging level')
    return parser.parse_args()


def setup_logging(log_level):
    \"\"\"Configure logging.\"\"\"
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level=log_level)
    logger.add('logs/comtrade_pipeline_{time}.log', rotation='100 MB', level=log_level)
    logger.info(f'Logging initialized at {log_level} level')


def main():
    \"\"\"Main function to run the pipeline.\"\"\"
    args = setup_argparse()
    setup_logging(args.log_level)
    
    config = load_config()
    logger.info('Starting Comtrade Data Pipeline')
    
    try:
        pipeline = ComtradePipeline(config)
        pipeline.run(
            countries=args.countries.split(',') if args.countries != 'all' else 'all',
            start_date=args.start_date,
            end_date=args.end_date
        )
        logger.info('Pipeline completed successfully')
    except Exception as e:
        logger.exception(f'Pipeline failed with error: {e}')
        sys.exit(1)
    
    sys.exit(0)


if __name__ == '__main__':
    main()