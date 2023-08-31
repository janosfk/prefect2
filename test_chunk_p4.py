import asyncio
import uuid
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any

import requests
#import prefect
from prefect import flow, task, get_run_logger, Task, unmapped
# from prefect_aws.batch import batch_submit
# from prefect_aws import AwsCredentials
# import prefect_utils.aws as _prefect_aws
# from prefect_utils.aws import wait_for_batch_job, wait_for_batch_jobs
# from itertools import chain
from itertools import cycle
import random


@task(retries=10, retry_delay_seconds=20)
async def batch_submit_stub(
        job_name: str,
        job_queue: str,
        job_definition: str,
        aws_credentials: str,
        batch_kwargs: str,
) -> str:
    logger = get_run_logger()

    guid = str(uuid.uuid4())
    await asyncio.sleep(random.randint(1, 4))
    logger.info(f"{job_name} done")
    return guid


@flow
async def process_chunk(chunk_batch_kwargs_list, chunk_job_names):
    logger = get_run_logger()
    kw_job_name_list = list(zip(chunk_batch_kwargs_list, cycle(chunk_job_names)))

    tasks = list(map(lambda t: batch_submit_stub.with_options(name=f"{t[1]}")
                     .submit(job_name=f"{t[1]}",
                             job_queue="job_queue",
                             job_definition="job_definition",
                             aws_credentials="aws_credentials",
                             batch_kwargs=t[0]
                             ), kw_job_name_list))

    tasks_results = await asyncio.gather(*tasks)

    logger.info(f"{tasks_results=}")


@flow
async def run_in_chunks(batch_kwargs_list: List[str],
                        job_names: List[str],
                        chunk_size: int):
    logger = get_run_logger()

    n = chunk_size

    chunk_batch_kwargs_list = [batch_kwargs_list[i * n:(i + 1) * n] for i in
                               range((len(batch_kwargs_list) + n - 1) // n)]
    chunk_job_names = [job_names[i * n:(i + 1) * n] for i in range((len(job_names) + n - 1) // n)]
    count_chunks = len(chunk_batch_kwargs_list)

    index = 1
    for chunk_b, chunk_j in zip(chunk_batch_kwargs_list, chunk_job_names):
        start = datetime.now()
        logger.info(f"START chunk : {index}/{count_chunks} - {start}")
        await process_chunk(chunk_batch_kwargs_list=chunk_b, chunk_job_names=chunk_j)
        end = datetime.now()
        elapsed = end - start
        logger.info(f"END:  chunk : {index}/{count_chunks} - {end} \n Elapsed : {elapsed}")
        index = index + 1

    return True


@task
async def call_api(url):
    response = requests.get(url)
    print(response.status_code)
    return response.json()


@flow
async def test_gh(total_size: int = 10, chunk_size: int = 5):
    logger = get_run_logger()
    url = "https://catfact.ninja/fact"
    fact_json = await call_api(url)
    logger.info(fact_json)

    return

    # logger = get_run_logger()
    # batch_kwargs_list = []
    # job_names = []
    # for index in range(total_size):
    #     batch_kwargs_list.append(f"kwarg:{index}")
    #     job_names.append(f"test_job:{index}")
    #
    # result = await run_in_chunks.fn(batch_kwargs_list=batch_kwargs_list, job_names=job_names, chunk_size=chunk_size)


if __name__ == "__main__":
    main_flow_state = asyncio.run(test_gh())
