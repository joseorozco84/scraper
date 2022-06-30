import aiohttp
import asyncio
from datetime import datetime


async def fetch(session, task):  # fetching urls and mark result of execution
    async with session.get(task['url']) as response:
        if response.status != 200:
            # response.raise_for_status()
            # Here you need to somehow  handle 429 code if it acquired
            # In my example I just skip it.
            task['result'] = response.status
            task['status'] = 'done'
        await response.text()  # just to be sure we acquire data
        print(f"{str(datetime.now())}: Got result of {task['url']}")  # logging
        task['result'] = response.status
        task['status'] = 'done'


async def fetch_all(session, urls, persecond):
    # convert to list of dicts
    url_tasks = [{'url': i, 'result': None, 'status': 'new'} for i in urls]
    n = 0  # counter
    while True:
        # calc how many tasks are fetching right now
        running_tasks = len([i for i in url_tasks if i['status'] in ['fetch']])
        # calc how many tasks are still need to be executed
        is_tasks_to_wait = len([i for i in url_tasks if i['status'] != 'done'])
        # check we are not in the end of list n < len()
        # check we have room for one more task
        if n < len(url_tasks) and running_tasks < persecond:
            url_tasks[n]['status'] = 'fetch'
            #
            # Here is main trick
            # If you schedule task inside running loop
            # it will start to execute sync code until find some await
            #
            asyncio.create_task(fetch(session, url_tasks[n]))
            n += 1
            print(f'Schedule tasks {n}. '
                  f'Running {running_tasks} '
                  f'Remain {is_tasks_to_wait}')
        # Check persecond constrain and wait a sec (or period)
        if running_tasks >= persecond:
            print('Throttling')
            await asyncio.sleep(1)
        #
        # Here is another main trick
        # To keep asyncio.run (or loop.run_until_complete) executing
        # we need to wait a little than check that all tasks are done and
        # wait and so on
        if is_tasks_to_wait != 0:
            await asyncio.sleep(0.1)  # wait all tasks done
        else:
            # All tasks done
            break
    return url_tasks


async def main():
    urls = ['https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page=0',
            'https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page=1',
            'https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page=0']*3
    async with aiohttp.ClientSession() as session:
        res = await fetch_all(session, urls, 3)
        print(res)

if __name__ == '__main__':
    asyncio.run(main())
    # (asyncio.run) do cancel all pending tasks (we do not have them,
    #  because we check all task done)
    # (asyncio.run) do await canceling all tasks
    # (asyncio.run) do stop loop
    # exit program