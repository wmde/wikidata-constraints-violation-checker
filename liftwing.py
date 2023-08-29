import asyncio
import logging
from collections import defaultdict
from typing import List

import aiohttp


async def get_liftwing_response(
    wiki_id: str,
    model_name: str,
    rev_id: int,
    features: bool,
    liftwing_url: str,
) -> dict:
    url = f"{liftwing_url}/v1/models/{wiki_id}-{model_name}:predict"

    data = {"rev_id": rev_id, "extended_output": features}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=data) as response:
                response_json = await response.json()
        except aiohttp.ClientError as e:
            logging.error(
                f"LiftWing call for model {model_name} and rev-id {rev_id} failed"
            )
        return response_json


def merge_liftwing_responses(wiki_id: str, responses: List[str]) -> defaultdict:
    result = defaultdict(lambda: defaultdict(lambda: defaultdict()))
    for d in responses:
        if not d:
            continue
        for k, v in d[wiki_id].items():
            if isinstance(v, dict) and k == "scores":
                for rev_id, scores in v.items():
                    if rev_id in result[wiki_id][k]:
                        result[wiki_id][k][rev_id].update(scores)
                    else:
                        result[wiki_id][k][rev_id] = scores
            else:
                result[wiki_id][k].update(v)
    return result


async def make_liftiwing_calls(
    wiki_id: str,
    models: List[str],
    rev_ids: List[int],
    features: bool = None,
    liftwing_url: str = "https://api.wikimedia.org/service/lw/inference",
):
    tasks = [
        get_liftwing_response(
            wiki_id=wiki_id,
            model_name=model,
            rev_id=revid,
            features=features,
            liftwing_url=liftwing_url,
        )
        for revid in rev_ids
        for model in models
    ]
    result = await asyncio.gather(*tasks)
    return merge_liftwing_responses(wiki_id, result)
