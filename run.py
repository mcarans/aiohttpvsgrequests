#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Compare aiohttp and grequests
'''
import logging
import hashlib
import asyncio
import time
import aiohttp
import grequests

from hdx.data.resource import Resource
from hdx.facades.simple import facade
from requests import HTTPError
from requests import Session
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

NUMBER_OF_URLS_TO_PROCESS = 100

async def fetch(metadata, session):
    url, resource_id = metadata
    md5hash = hashlib.md5()
    try:
        with aiohttp.Timeout(300, loop=session.loop):
            async with session.get(url, timeout=10) as response:
                last_modified = response.headers.get('Last-Modified', None)
                if last_modified:
                    return resource_id, url, 1, last_modified
                logger.info('Hashing %s' % url)
                async for chunk in response.content.iter_chunked(1024):
                    if chunk:
                        md5hash.update(chunk)
                return resource_id, url, 2, md5hash.hexdigest()
    except Exception as e:
        return resource_id, url, 0, str(e)


async def bound_fetch(sem, metadata, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(metadata, session)


async def aiohttp_check_resources_for_last_modified(last_modified_check, loop):
    tasks = list()

    # create instance of Semaphore
    sem = asyncio.Semaphore(100)

    conn = aiohttp.TCPConnector(keepalive_timeout=10, limit=100)
    async with aiohttp.ClientSession(connector=conn, loop=loop) as session:
        for metadata in last_modified_check:
            task = bound_fetch(sem, metadata, session)
            tasks.append(task)
        return await asyncio.gather(*tasks)


def set_metadata(metadata):
    def hook(resp, **kwargs):
        resp.metadata = metadata
        return resp
    return hook


def grequests_check_resources_for_last_modified(last_modified_check):
    results = list()
    reqs = list()

    def exception_handler(req, exc):
        url, res_id = req.metadata
        results.append((res_id, url, 0, str(exc)))

    with Session() as session:
        session.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
        session.mount('https://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
        for metadata in last_modified_check:
            req = grequests.get(metadata[0], timeout=10, session=session, callback=set_metadata(metadata))
            req.metadata = metadata
            reqs.append(req)
        for response in grequests.imap(reqs, size=100, stream=True, exception_handler=exception_handler):
            url, resource_id = response.metadata
            try:
                response.raise_for_status()
            except HTTPError as e:
                results.append((resource_id, url, 0, response.status_code))
                response.close()
                continue
            last_modified = response.headers.get('Last-Modified', None)
            if last_modified:
                results.append((resource_id, url, 1, last_modified))
                response.close()
                continue
            logger.info('Hashing %s' % url)
            md5hash = hashlib.md5()
            try:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        md5hash.update(chunk)
                results.append((resource_id, url, 2, md5hash.hexdigest()))
            except Exception as e:
                results.append((resource_id, url, 0, str(e)))
            finally:
                response.close()
    return results


def print_results(results):
    lastmodified_count = 0
    hash_count = 0
    failed_count = 0
    for resource_id, url, status, result in results:
        if status == 0:
            failed_count += 1
            logger.error(result)
        elif status == 1:
            lastmodified_count += 1
        elif status == 2:
            hash_count += 1
        else:
            raise ValueError('Invalid status returned!')
    str = 'Have Last-Modified: %d, Hashed: %d, ' % (lastmodified_count, hash_count)
    str += 'Number Failed: %d' % failed_count
    logger.info(str)


def run_aiohttp(last_modified_check):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(aiohttp_check_resources_for_last_modified(last_modified_check, loop))
    results = loop.run_until_complete(future)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    print_results(results)


def run_grequests(last_modified_check):
    start_time = time.time()
    results = grequests_check_resources_for_last_modified(last_modified_check)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    print_results(results)


def main(configuration):
    resources = Resource.search_in_hdx(configuration, 'name:')
    last_modified_check = list()
    for resource in resources:
        resource_id = resource['id']
        url = resource['url']
        if 'data.humdata.org' in url or 'manage.hdx.rwlabs.org' in url or 'proxy.hxlstandard.org' in url or \
                'scraperwiki.com' in url or 'ourairports.com' in url:
            continue
        last_modified_check.append((url, resource_id))
    last_modified_check = sorted(last_modified_check)[:NUMBER_OF_URLS_TO_PROCESS]
#    run_grequests(last_modified_check)
    run_aiohttp(last_modified_check)

if __name__ == '__main__':
    facade(main, hdx_site='prod', hdx_read_only=True)
