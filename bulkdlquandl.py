import itertools
import logging
import io
import os
import asyncio as aio
import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('asyncio').setLevel(logging.WARNING)  # tone down asyncio debug messages


from settings import DL_DIR, AUTH_TOKEN, MAX_CONN


class QuandlUpdater(object):

    def __init__(self):
        self._dl_codes_queue = aio.Queue()

    @aio.coroutine
    def dl_quandl_wiki(self):
        processors = [aio.async(self.process_source_datasets())]
        for i in range(MAX_CONN):
            fut = aio.async(self.process_dl_codes_queue(i))
            processors.append(fut)
        logger.debug('All processors online.')
        print_task = aio.async(self.print_queue_status())
        yield from aio.wait(processors)
        logger.debug('All processors shut down.')
        print_task.cancel()

    @aio.coroutine
    def process_source_datasets(self):
        dfs = []
        for page_num in itertools.count(start=1):
            logger.debug('Downloading list page %d' % page_num)
            url = 'http://www.quandl.com/api/v2/datasets.csv?query=*&source_code=WIKI&per_page=300&page=%d&auth_token=%s' % (page_num, AUTH_TOKEN)
            r = yield from aiohttp.request('get', url)
            text = yield from r.text()
            if len(text) == 0:
                break
            df = pd.read_csv(io.StringIO(text), header=None, names=['code', 'name', 'start_date', 'end_date', 'frequency', 'last_updated'])
            for code in df['code']:
                yield from self._dl_codes_queue.put(code)
            dfs.append(df)

        concat_df = pd.concat(dfs)
        concat_df.to_csv('dataset.WIKI.csv')
        logger.debug('Finished downloading list.')
        yield from self._dl_codes_queue.put(None)  # put the stop sign

    @aio.coroutine
    def process_dl_codes_queue(self, id):
        logger.debug('Starting processor #%d' % id)
        while True:
            code = yield from self._dl_codes_queue.get()
            if code is None:
                yield from self._dl_codes_queue.put(None)  # put back the stop sign to let other coroutines see it
                logger.debug('Shutting down processor #%d' % id)
                return
            else:
                fp = os.path.join(DL_DIR, '%s.csv' % code.replace('/', '.'))
                if os.path.isfile(fp):
                    logger.debug('Processor #%d: skipping %s as it is already downloaded' % (id, code))
                    continue
                logger.debug('Processor #%d: processing %s' % (id, code))
                url = 'http://www.quandl.com/api/v1/datasets/%s.csv?sort_order=asc&auth_token=%s' % (code, AUTH_TOKEN)
                r = yield from aiohttp.request('get', url)
                text = yield from r.text()
                df = pd.read_csv(io.StringIO(text), index_col=0)
                df.to_csv(fp)
                logger.debug('Processor #%d: finished processing %s' % (id, code))

    @aio.coroutine
    def print_queue_status(self):
        while True:
            logger.info('Queue length: %d' % self._dl_codes_queue.qsize())
            yield from aio.sleep(3)


if __name__ == "__main__":
    if not os.path.isdir(DL_DIR):
        os.makedirs(DL_DIR)
    updater = QuandlUpdater()
    aio.get_event_loop().run_until_complete(updater.dl_quandl_wiki())
