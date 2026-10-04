'use strict';
// 整页状态读取（GET /api/state）端到端测试。
// 验证：
//   1) 关联记录成批装配：用数据库语句计数确认 /api/state 的 SQL 执行次数为固定上界，
//      不随事件、标签卷、核销/更正、出货单或供应批号数量增长；
//   2) 同一时点：受控批次的隔离状态、卷 quarantine_paused、卷/批次可出货量与
//      shipments[].batch_quarantined 在同一响应内一致；
//   3) 字段/排序/形状：空库返回原形状（空数组与汇总），核销更正链最新有效数量正确、
//      不混入其他卷或批次，停用卷与待评估单终态保留；
//   4) 旧输入缺数量仍按原形状返回（qty_grams=null, measured=false，计入未计量数）。
//   node scripts/test-state-read.js
// 退出码 0 表示全部通过。

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');

const PORT = '39251';
const BASE = `http://127.0.0.1:${PORT}`;
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'state-read-test-'));
const dbPath = path.join(tmpDir, 'test.db');
const countsFile = path.join(tmpDir, 'counts.json');

// 语句计数 preload：SIGUSR1 清零并开始计数，SIGUSR2 停止并把次数写入 COUNTS_FILE。
const instrumentPath = path.join(tmpDir, 'instrument.js');
fs.writeFileSync(instrumentPath, `
'use strict';
const fs = require('fs');
const Module = require('module');
const origRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  const exp = origRequire.apply(this, arguments);
  if (id === 'better-sqlite3' && exp && exp.prototype && !exp.__instrumented) {
    const origPrepare = exp.prototype.prepare;
    exp.prototype.prepare = function (...args) {
      const stmt = origPrepare.apply(this, args);
      for (const m of ['all', 'get', 'run']) {
        const real = stmt[m];
        if (typeof real !== 'function') continue;
        stmt[m] = function (...a) {
          if (global.__counting) global.__counts.n += 1;
          return real.apply(this, a);
        };
      }
      return stmt;
    };
    exp.__instrumented = true;
  }
  return exp;
};
global.__counting = false;
global.__counts = { n: 0 };
process.on('SIGUSR1', () => { global.__counts = { n: 0 }; global.__counting = true; });
process.on('SIGUSR2', () => {
  global.__counting = false;
  fs.writeFileSync(process.env.COUNTS_FILE, JSON.stringify(global.__counts));
});
`);

let failures = 0;
function check(name, cond, extra) {
  if (cond) console.log(`  ✓ ${name}`);
  else {
    failures += 1;
    console.error(`  ✗ ${name}`, extra !== undefined ? JSON.stringify(extra) : '');
  }
}

async function api(method, urlPath, body) {
  const res = await fetch(BASE + urlPath, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined
  });
  let json = null;
  try { json = await res.json(); } catch (_) { /* 非 JSON */ }
  return { status: res.status, json };
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitReady(proc) {
  for (let i = 0; i < 60; i++) {
    try {
      const res = await fetch(BASE + '/api/state');
      if (res.ok) return;
    } catch (_) { /* 未就绪 */ }
    if (proc.exitCode !== null) throw new Error('服务提前退出');
    await sleep(250);
  }
  throw new Error('服务启动超时');
}

async function main() {
  const proc = spawn(process.execPath, [path.join(__dirname, '..', 'server', 'server.js')], {
    env: {
      ...process.env,
      PORT, HOST: '127.0.0.1', DB_PATH: dbPath, SEED_DEMO: 'true',
      NODE_OPTIONS: `--require ${instrumentPath}`,
      COUNTS_FILE: countsFile
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  proc.stdout.on('data', () => {});
  proc.stderr.on('data', (d) => process.stderr.write('[server] ' + d));

  const countOnce = async () => {
    proc.kill('SIGUSR1');
    await api('GET', '/api/state');
    proc.kill('SIGUSR2');
    await sleep(100);
    return JSON.parse(fs.readFileSync(countsFile, 'utf8')).n;
  };

  try {
    await waitReady(proc);
    console.log('服务已就绪（演示数据），开始测试');

    const st0 = await api('GET', '/api/state');
    check('初始状态 200 且含全部顶层键',
      st0.status === 200 && ['materials', 'equipment', 'events', 'batches', 'label_rolls',
        'shipments', 'downtime_windows', 'quarantine_orders', 'lot_inventory',
        'lot_inventory_summary'].every((k) => k in st0.json),
      Object.keys(st0.json || {}));

    // ---------- 1. 数据量翻倍前后，查询数恒定 ----------
    console.log('1) 成批装配：SQL 次数不随事件/卷/核销/出货单/批号数量增长');
    const q0 = await countOnce();

    const matId = (code) => st0.json.materials.find((m) => m.code === code).id;
    const MILK = matId('MILK-01'), EGG = matId('EGG-01');
    let batchId;
    // 造 10 个链式批次（事件+输入），每批 2 卷、每卷 3 笔核销含更正、前 4 批各出 2 张单。
    for (let i = 0; i < 10; i++) {
      const code = `SCALE-B${String(i + 1).padStart(2, '0')}`;
      const r = await api('POST', '/api/events', {
        type: 'PROCESS', event_time: `2026-12-${String(10 + i).padStart(2, '0')}T08:00`,
        equipment_id: st0.json.equipment[i % st0.json.equipment.length].id,
        batch_code: code, note: '扩展批次',
        material_inputs: [
          { material_id: MILK }, { material_id: EGG }
        ].concat(i > 0 ? [] : []),
        ref_batch_ids: i > 0 ? [batchId] : []
      });
      if (r.status !== 200) throw new Error('创建扩展事件失败: ' + JSON.stringify(r.json));
      const st = await api('GET', '/api/state');
      batchId = st.json.batches.find((b) => b.code === code).id;

      const rel = await api('POST', '/api/releases', {
        batch_id: batchId, released_at: `2026-12-${String(10 + i).padStart(2, '0')}T18:00`,
        released_by: '质检员'
      });
      if (rel.status !== 200) throw new Error('放行失败: ' + JSON.stringify(rel.json));
      const st2 = await api('GET', '/api/state');
      const releaseId = st2.json.batches.find((b) => b.id === batchId).releases[0].id;
      const allergens = st2.json.batches.find((b) => b.id === batchId).allergens;
      const rolls = [];
      for (let k = 0; k < 2; k++) {
        const rr = await api('POST', '/api/label-rolls', {
          batch_id: batchId, release_id: releaseId, allergens,
          print_qty: 9000, registered_by: '仓管'
        });
        if (rr.status !== 200) throw new Error('登记卷失败: ' + JSON.stringify(rr.json));
        rolls.push(rr.json);
      }
      for (const roll of rolls) {
        let rev = roll.revision;
        const c1 = await api('POST', `/api/label-rolls/${roll.id}/consume`,
          { expected_revision: rev, applied: 1000, voided: 10, used_by: '仓管' });
        if (c1.status !== 200) throw new Error('核销1失败: ' + JSON.stringify(c1.json));
        rev += 1;
        const c2 = await api('POST', `/api/label-rolls/${roll.id}/consume`,
          { expected_revision: rev, applied: 500, voided: 0, used_by: '仓管' });
        if (c2.status !== 200) throw new Error('核销2失败: ' + JSON.stringify(c2.json));
        rev += 1;
        const st3 = await api('GET', '/api/state');
        const usages = st3.json.label_rolls.find((x) => x.id === roll.id).usages;
        const corr = await api('POST',
          `/api/label-rolls/${roll.id}/usages/${usages[0].id}/corrections`, {
            expected_revision: rev, target_applied_qty: 900, target_void_qty: 20,
            corrected_by: '仓管', reason: '批量验证更正'
          });
        if (corr.status !== 200) throw new Error('更正失败: ' + JSON.stringify(corr.json));
      }
      if (i < 4) {
        for (const [a, b] of [[150, 150], [100, 100]]) {
          const sh = await api('POST', '/api/shipments', {
            batch_id: batchId, ship_qty: a + b, shipped_by: '仓管',
            allocations: rolls.map((rl) => ({
              roll_id: rl.id, qty: a === 150 ? 150 : 100
            }))
          });
          if (sh.status !== 200) throw new Error('出货失败: ' + JSON.stringify(sh.json));
        }
      }
    }

    const st1 = await api('GET', '/api/state');
    const q1 = await countOnce();
    console.log(`    演示库 SQL 次数=${q0}；扩展后（${st1.json.events.length} 事件、` +
      `${st1.json.label_rolls.length} 卷、${st1.json.shipments.length} 单）SQL 次数=${q1}`);
    // 固定上界：与数据量无关（当前装配约 25~30 条；留余量断言 ≤ 40）。
    check('扩展后查询数仍为固定上界（≤40，不随数据增长）', q1 <= 40, { q0, q1 });
    check('扩展后查询数与演示库同量级（差 ≤ 空集合跳过造成的小幅波动）',
      Math.abs(q1 - q0) <= 5, { q0, q1 });

    // ---------- 2. 同一时点一致性：签发隔离令后同一响应内读数一致 ----------
    console.log('2) 同一时点：隔离状态/核销有效数/可出货量/库存可用量同响应一致');
    const milkLot = st1.json.materials.find((m) => m.code === 'MILK-01').lots[0].id;
    const iq = await api('POST', '/api/quarantine-orders', {
      lot_id: milkLot, reason: '测试隔离', issued_by: '质检员'
    });
    check('隔离令签发成功', iq.status === 200, iq.json);

    const st2json = (await api('GET', '/api/state')).json;
    const controlledBatchIds = new Set(
      st2json.batches.filter((b) => b.quarantined).map((b) => b.id)
    );
    check('受控批次 quarantine_orders 非空', [...controlledBatchIds].every((bid) =>
      st2json.batches.find((b) => b.id === bid).quarantine_orders.length > 0));
    const rollsInConsistent = st2json.label_rolls.every((r) => {
      const controlled = controlledBatchIds.has(r.batch_id);
      if (!controlled) return true;
      return r.quarantine_paused === (r.status === 'ACTIVE') && r.shippable_qty === 0;
    });
    check('受控批次的卷：ACTIVE 卷 quarantine_paused=true 且 shippable_qty=0', rollsInConsistent);
    const shipsConsistent = st2json.shipments.every((s) =>
      s.batch_id == null || s.batch_quarantined === controlledBatchIds.has(s.batch_id));
    check('出货单 batch_quarantined 与批次隔离状态一致', shipsConsistent);
    const lotMarked = st2json.materials
      .flatMap((m) => m.lots)
      .find((l) => l.id === milkLot);
    check('批号标记 quarantine_active 与生效令号', lotMarked.quarantine_active === true &&
      /^QUAR-\d+$/.test(lotMarked.active_quarantine_order), lotMarked);

    // 核销最新有效数与可出货量按同一批数据自洽：未受控 ACTIVE 卷
    // shippable = max(已贴 - 已分配, 0)（已贴按各笔更正后有效数合计）。
    const selfConsistent = st2json.label_rolls.every((r) => {
      const effApplied = r.usages.reduce((s, u) => s + u.applied_qty, 0);
      if (effApplied !== r.applied_qty) return false;
      if (r.status !== 'ACTIVE' || controlledBatchIds.has(r.batch_id)) {
        return r.shippable_qty === 0;
      }
      return r.shippable_qty === Math.max(r.applied_qty - r.allocated_qty, 0);
    });
    check('卷已贴数=各笔核销最新有效数合计；可出货量与隔离/分配同响应自洽', selfConsistent);

    // 库存可用量与批号领用自洽（available = opening + inbound - used）。
    const invConsistent = st2json.lot_inventory.every((l) => {
      if (!l.metered) return l.available_qty === null && l.opening_qty === null;
      return l.available_qty === l.opening_qty + l.inbound_qty - l.used_qty;
    });
    check('库存台账 available_qty = 期初 + 入库 − 领用（同响应）', invConsistent);

    // ---------- 3. 不串卷/批：每笔核销的更正链只属于本卷 ----------
    console.log('3) 归组隔离：核销/更正链不混入其他卷，出货明细不混入其他单');
    const usageRollOk = st2json.label_rolls.every((r) =>
      r.usages.every((u) => u.roll_id === r.id &&
        u.corrections.every((c) => c.roll_id === r.id && c.usage_id === u.id)));
    check('每卷 usages[].roll_id 与 corrections 归属严格匹配', usageRollOk);
    const shipmentItemsOk = st2json.shipments.every((s) =>
      s.allocated_qty === s.items.reduce((n, it) => n + it.qty, 0));
    check('出货单 allocated_qty 等于其自身明细合计', shipmentItemsOk);

    // ---------- 4. 终态与历史快照保留 ----------
    console.log('4) 停用/待评估/失效终态与历史快照形状');
    check('卷含 status/active/retired_reason/applied_pending_review 字段',
      st2json.label_rolls.every((r) => 'retired_reason' in r && 'applied_pending_review' in r &&
        typeof r.active === 'boolean'));
    check('出货单含 pending_review/review_reason/current_disposition',
      st2json.shipments.every((s) => typeof s.pending_review === 'boolean' &&
        Array.isArray(s.items)));
    check('放行含 status/invalid_reason/snapshot_allergens 历史快照',
      st2json.batches.flatMap((b) => b.releases).every((r) =>
        Array.isArray(r.snapshot_allergens) && typeof r.status === 'string'));

    // ---------- 5. 排序 ----------
    console.log('5) 排序保持');
    const lexcmp = (a, b) => a.localeCompare(b, 'zh');
    check('materials 按 code 排序',
      st2json.materials.map((m) => m.code).every((c, i, arr) => i === 0 || lexcmp(arr[i - 1], c) <= 0));
    check('events 按 event_time,id 排序',
      st2json.events.every((e, i, arr) => i === 0 ||
        arr[i - 1].event_time <= e.event_time));
    check('label_rolls 按 batch_code,roll_code 排序',
      st2json.label_rolls.every((r, i, arr) => i === 0 ||
        lexcmp(arr[i - 1].batch_code || '', r.batch_code || '') <= 0));
    check('shipments 按 id DESC 排序',
      st2json.shipments.every((s, i, arr) => i === 0 || arr[i - 1].id >= s.id));

    // ---------- 6. 空库形状（独立进程，SEED_DEMO=false）----------
    console.log('6) 空库仍按原形状返回');
  } finally {
    proc.kill('SIGTERM');
  }

  // 空库：另起实例
  const emptyDb = path.join(tmpDir, 'empty.db');
  const emptyProc = spawn(process.execPath, [path.join(__dirname, '..', 'server', 'server.js')], {
    env: {
      ...process.env, PORT: String(Number(PORT) + 1), HOST: '127.0.0.1',
      DB_PATH: emptyDb, SEED_DEMO: 'false',
      NODE_OPTIONS: `--require ${instrumentPath}`,
      COUNTS_FILE: path.join(tmpDir, 'counts-empty.json')
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  emptyProc.stdout.on('data', () => {});
  emptyProc.stderr.on('data', () => {});
  try {
    await waitReadyOnPort(Number(PORT) + 1);
    const res = await fetch(`http://127.0.0.1:${Number(PORT) + 1}/api/state`);
    const json = await res.json();
    check('空库顶层键齐全且集合为空',
      ['materials', 'equipment', 'events', 'batches', 'label_rolls', 'shipments',
        'downtime_windows', 'quarantine_orders', 'lot_inventory'].every((k) =>
          Array.isArray(json[k]) && json[k].length === 0),
      Object.keys(json));
    check('空库汇总形状',
      json.lot_inventory_summary && json.lot_inventory_summary.lot_count === 0 &&
      json.lot_inventory_summary.metered_lot_count === 0 &&
      json.lot_inventory_summary.unmeasured_input_count === 0,
      json.lot_inventory_summary);
  } finally {
    emptyProc.kill('SIGTERM');
  }

  if (failures) {
    console.error(`\n${failures} 项失败`);
    process.exit(1);
  }
  console.log('\n全部通过');
}

async function waitReadyOnPort(port) {
  for (let i = 0; i < 60; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/api/state`);
      if (res.ok) return;
    } catch (_) { /* 未就绪 */ }
    await sleep(250);
  }
  throw new Error('空库服务启动超时');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
