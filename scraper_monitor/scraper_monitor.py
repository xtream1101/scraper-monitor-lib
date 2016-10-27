import logging
import requests
import datetime
import threading

logger = logging.getLogger(__name__)


class ScraperMonitor():

    def __init__(self):
        self.config = {}

    def start(self, scraper_name=None, host=None, apikey=None, scraper_key=None, scraper_run=None, environment=None, machine_name=None):
        """
        Log that the scraper has started
        Pass the unique data for this scraper instance like `scraper_run` which is the scrape_id
        """
        # requests needs the host to start with http(s)
        if not host.startswith('http'):
            host = 'http://' + host

        self.config['scraper_name'] = scraper_name
        self.config['host'] = host
        self.config['apikey'] = apikey
        self.config['scraper_key'] = scraper_key
        self.config['scraper_run'] = scraper_run
        self.config['environment'] = environment
        self.config['machine_name'] = machine_name

        # Tell the server we started the scraper
        scraper_data = {'startTime': str(datetime.datetime.utcnow())}
        self._send('/data/start', scraper_data)

    def stop(self, total_urls=None, ref_data_success_count=None, ref_data_count=None, rows_added_to_db=None):
        """
        Log that the scraper has stopped
        """
        scraper_data = {'stopTime': str(datetime.datetime.utcnow()),
                        'totalUrls': total_urls,
                        'refDataSuccessCount': ref_data_success_count,
                        'refDataCount': ref_data_count,
                        'rowsAddedToDb': rows_added_to_db,
                        }
        self._send('/data/stop', scraper_data)

    def _send(self, endpoint, data={}):
        """
        Takes care of sending the data to the Scraper Monitor
        """
        if len(self.config) == 0:
            logger.warning("Cannot send data, scraper monitor never started")
            return

        if self.config.get('host') is None:
            logger.critical("Must provide a host for the Scraper Monitor")
            return

        if endpoint.startswith('/'):
            endpoint = endpoint[1:]

        if self.config['host'].endswith('/'):
            self.config['host'] = self.config['host'][:-1]

        server_url = "{host}/api/v1/{ep}?apikey={apikey}&scraperKey={scraper_key}&scraperRun={run_id}&environment={env}"\
                     .format(host=self.config['host'],
                             ep=endpoint,
                             apikey=self.config['apikey'],
                             scraper_key=self.config['scraper_key'],
                             run_id=self.config['scraper_run'],
                             env=self.config['environment'],
                             )
        try:
            r = requests.post(server_url, json=data, timeout=10).json()
            if r.get('success') is not True:
                logger.error("Failed to send data to Scraper Monitor {} - {} "
                             .format(endpoint, r.get('message')))
        except KeyError:
            logger.exception(r.get('message'))

        except requests.exceptions.Timeout:
            logger.exception("Request timeout while sending scraper data")

        except Exception:
            logger.exception("Something broke in sending scraper data")

    ###########################################################################
    ###########################################################################
    ##
    ##  Log other values
    ##
    ###########################################################################
    ###########################################################################
    def failed_url(self, url, reason, ref_id=None, ref_table=None, status_code=None, num_tries=None):
        """
        Log any urls that did not meet the requirements of being successful
        """
        thread_name = threading.current_thread().name

        scraper_data = {'url': url,
                        'reason': reason,
                        'ref_id': ref_id,
                        'ref_table': ref_table,
                        'statusCode': status_code,
                        'numTries': num_tries,
                        'threadName': thread_name,
                        }
        self._send('/error/url', scraper_data)
