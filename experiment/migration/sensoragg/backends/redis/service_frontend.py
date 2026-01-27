#!/usr/bin/env python3
# Simple HTTP frontend for Redis sensoragg service
# Provides /write (POST) and /read (GET) endpoints used by benches when --frontend-url is set

from flask import Flask, request, jsonify
import redis
import os
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_frontend")

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

@app.route('/write', methods=['POST'])
def write():
    data = request.get_json(force=True)
    key = data.get('key')
    member = data.get('member')
    score = data.get('score')
    if not key or member is None or score is None:
        return jsonify({'error': 'missing key/member/score'}), 400
    try:
        # Score must be float
        s = float(score)
        r.zadd(key, {member: s})
        return jsonify({'ok': True}), 200
    except Exception as e:
        logger.exception("Write failed")
        return jsonify({'error': str(e)}), 500

@app.route('/read', methods=['GET'])
def read():
    key = request.args.get('key')
    qtype = request.args.get('query', 'range')
    try:
        now = int(request.args.get('now') or 0)
        if qtype == 'range':
            if not now:
                now = int(__import__('time').time() * 1000)
            results = r.zrangebyscore(key, now-60000, now, withscores=True)
            # decode bytes -> utf-8 where possible
            out = []
            for member, score in results:
                try:
                    out.append({'member': member.decode('utf-8'), 'score': score})
                except Exception:
                    out.append({'member': str(member), 'score': score})
            return jsonify(out)
        elif qtype == 'count':
            if not now:
                now = int(__import__('time').time() * 1000)
            cnt = r.zcount(key, now-60000, now)
            return jsonify({'count': cnt})
        elif qtype == 'min':
            res = r.zrange(key, 0, 0, withscores=True)
            return jsonify([{'member': res[0][0].decode('utf-8'), 'score': res[0][1]}] if res else [])
        else:  # max
            res = r.zrange(key, -1, -1, withscores=True)
            return jsonify([{'member': res[0][0].decode('utf-8'), 'score': res[0][1]}] if res else [])
    except Exception as e:
        logger.exception("Read failed")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('FRONTEND_PORT', 5000))
    app.run(host='0.0.0.0', port=port)
