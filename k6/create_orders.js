import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 10,
  duration: '30s',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const payload = JSON.stringify({
    customer_id: `cust-${__VU}-${__ITER}`,
    total_amount: Number((Math.random() * 500 + 10).toFixed(2)),
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(`${BASE_URL}/orders`, payload, params);

  check(res, {
    'status is 201 Created': (r) => r.status === 201,
    'response has order id': (r) => r.json('id') !== undefined,
  });

  sleep(0.5);
}
