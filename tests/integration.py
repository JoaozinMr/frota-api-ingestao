"""Testes reais via HTTP + confirmação de uma mensagem no Redpanda.
Execute após docker compose up --build -d, na raiz do projeto:
python tests/integration.py

Teste ponta a ponta opcional (exige a stack de integração):
docker compose -f compose.yaml -f compose.integration.yaml up --build -d
E2E=1 python tests/integration.py
"""
import json
import os
import subprocess
import time
import unittest
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = os.environ.get('API_URL', 'http://localhost:8081')
ALERTAS = os.environ.get('ALERTAS_URL', 'http://localhost:5100')
E2E = os.environ.get('E2E') == '1'


def payload():
    return {
        'id_evento': str(uuid.uuid4()), 'id_veiculo': 'CAM-TESTE',
        'sequencia': 1, 'timestamp': datetime.now(timezone.utc).isoformat(),
        'latitude': -19.9191, 'longitude': -43.9386,
        'velocidade': 72.5, 'motor_ligado': True,
    }


def send(data, correlation=None):
    raw = data if isinstance(data, bytes) else json.dumps(data).encode()
    headers = {'Content-Type': 'application/json'}
    if correlation:
        headers['X-Correlation-ID'] = correlation
    request = Request(BASE + '/api/telemetry', data=raw, headers=headers, method='POST')
    try:
        with urlopen(request, timeout=20) as response:
            return response.status, json.load(response), response.headers
    except HTTPError as response:
        return response.code, json.load(response), response.headers


def rpk(*args, timeout=30):
    return subprocess.run(
        ['docker', 'compose', 'exec', '-T', 'redpanda', 'rpk', *args],
        capture_output=True, text=True, check=True, timeout=timeout).stdout


def stored_event(body):
    """Lê do broker a mensagem na partição/offset devolvidos pela API."""
    return json.loads(rpk(
        'topic', 'consume', 'frota.telemetria', '-p', str(body['particao']),
        '-o', str(body['offset']), '-n', '1', '--format', '%v'))


class IntegrationTests(unittest.TestCase):
    def test_valid_event_reaches_broker(self):
        event = payload()
        correlation = str(uuid.uuid4())
        status, body, headers = send(event, correlation)
        self.assertEqual(202, status)
        self.assertEqual(correlation, headers['X-Correlation-ID'])
        self.assertEqual(event['id_evento'], body['id_evento'])
        stored = stored_event(body)
        self.assertEqual(event['id_evento'], stored['id_evento'])
        self.assertEqual(event['id_veiculo'], stored['id_veiculo'])
        # Contrato com o Worker: status_motor textual derivado de motor_ligado.
        self.assertEqual('LIGADO', stored['status_motor'])
        self.assertTrue(stored['motor_ligado'])

    def test_engine_off_is_published_as_desligado(self):
        event = payload()
        event['motor_ligado'] = False
        status, body, _ = send(event)
        self.assertEqual(202, status)
        stored = stored_event(body)
        self.assertEqual('DESLIGADO', stored['status_motor'])
        self.assertFalse(stored['motor_ligado'])

    def test_dlq_message_has_origin_headers(self):
        # Margem para diferença de relógio entre host e container.
        since_ms = int(time.time() * 1000) - 5000
        correlation = str(uuid.uuid4())
        self.assertEqual(400, send(b'{broken', correlation)[0])
        out = rpk('topic', 'consume', 'frota.telemetria.dlq', '-o', f'@{since_ms}:end',
                  '--format', '%k\t%h{%k=%v;}\n')
        lines = [l for l in out.splitlines() if l.startswith(correlation + '\t')]
        self.assertEqual(1, len(lines), out)
        self.assertIn('dlq-origin=frota-api-ingestao', lines[0])
        self.assertIn('dlq-exception-type=JsonException', lines[0])
        self.assertIn(f'X-Correlation-ID={correlation}', lines[0])

    def test_invalid_coordinate_is_rejected(self):
        event = payload()
        event['latitude'] = 999
        status, body, _ = send(event)
        self.assertEqual(400, status)
        self.assertEqual('rejeitado', body['status'])

    def test_broken_json_is_rejected(self):
        self.assertEqual(400, send(b'{broken')[0])

    def test_missing_numeric_field_is_rejected(self):
        event = payload()
        del event['latitude']
        self.assertEqual(400, send(event)[0])

    def test_zero_and_false_are_valid(self):
        event = payload()
        event.update(latitude=0, longitude=0, velocidade=0, motor_ligado=False)
        self.assertEqual(202, send(event)[0])

    def test_delayed_event_is_accepted(self):
        event = payload()
        event['timestamp'] = '2020-01-01T00:00:00Z'
        self.assertEqual(202, send(event)[0])

    def test_timestamp_without_timezone_is_rejected(self):
        event = payload()
        event['timestamp'] = '2026-10-06T12:00:00'
        self.assertEqual(400, send(event)[0])


@unittest.skipUnless(E2E, 'defina E2E=1 com compose.integration.yaml em execução')
class EndToEndTests(unittest.TestCase):
    def test_speeding_reaches_alert_api_with_same_correlation_id(self):
        event = payload()
        # Veículo único para não colidir com a deduplicação do Worker (5 min por veículo/tipo).
        event.update(id_veiculo=f'E2E-{uuid.uuid4().hex[:12]}', velocidade=130.0)
        correlation = str(uuid.uuid4())
        status, _, _ = send(event, correlation)
        self.assertEqual(202, status)

        query = urlencode({'id_veiculo': event['id_veiculo'],
                           'tipo_infracao': 'EXCESSO_VELOCIDADE', 'limit': 10})
        deadline = time.time() + 30
        alerts = []
        while time.time() < deadline and not alerts:
            try:
                with urlopen(f'{ALERTAS}/api/alerts?{query}', timeout=5) as response:
                    alerts = json.load(response)
            except OSError:
                pass
            if not alerts:
                time.sleep(1)
        self.assertTrue(alerts, 'alerta EXCESSO_VELOCIDADE não chegou à API de Alertas')
        self.assertEqual(correlation, alerts[0]['correlation_id'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
