# 导入logging模块，并设置级别为INFO
import logging
logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web
# 返回一个页面
def index(request):
    return web.Response(body=b'<h1>Awesome</h1>', content_type='text/html')

async def init(loop):
    # 创建一个网站应用
    app = web.Application(loop=loop)
    # 为app添加一个网页
    app.router.add_route('GET', '/', index)
    # 创建一个监听连接
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()