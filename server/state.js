'use strict';
// 整页状态读取（GET /api/state）。
//
// 一次性返回：原料及供应批号、设备与事件（含清洗回放信息与补录引用）、批次放行、
// 标签卷核销及更正链、出货单、隔离令与库存台账。
//
// 性能：关联记录全部成批装配——材料批号、事件输入、批次放行、卷核销与更正链、
//   出货单明细/处置、停机窗口冲突、隔离令范围与库存台账均用固定数量的批量查询，
//   查询次数不随事件、标签卷、核销、出货单或供应批号数量逐笔增长。
//
// 同一时点：整个装配过程运行在单个延迟（只读）事务中，SQLite 为事务提供语句级
//   快照——同一响应中的核销有效数、可出货量、隔离状态与库存可用量必然对应同一时点，
//   不会读到并发写入的中间状态。
//
// 字段、排序、历史快照与终态语义与原逐笔装配完全一致（停用卷 RETIRED、待评估单
//   PENDING_REVIEW、失效放行 INVALIDATED 均只读呈现，不做解冻/恢复）；空库、无关联
//   记录或旧输入缺数量时仍按原形状返回，归组严格按外键 id，不混入其他卷或批次的数据。

const labels = require('./labels');
const shipments = require('./shipments');
const quarantine = require('./quarantine');
const downtime = require('./downtime');
const inventory = require('./inventory');
const { replayTimeline } = require('./recompute');

function getPageState(db) {
  return db.transaction(() => {
    // ---- 质量隔离：同一事务一次取数，批次->生效令 与 受控批次集合共用 ----
    const { quarByBatch, controlledBatchIds } = quarantine.controlSnapshot(db);
    const quarByLot = quarantine.activeOrdersByLot(db);

    // ---- 批号库存台账（固定批量查询，含未计量旧输入数）----
    const inventoryData = inventory.allBalances(db);
    const invByLot = new Map(inventoryData.lots.map((l) => [l.lot_id, l]));
    const inventoryBrief = (lotId) => {
      const b = invByLot.get(lotId);
      if (!b) {
        return { metered: false, opening_qty: null, inbound_qty: null, used_qty: 0,
          available_qty: null, unmeasured_input_count: 0 };
      }
      return {
        metered: b.metered,
        opening_qty: b.opening_qty,
        inbound_qty: b.inbound_qty,
        used_qty: b.used_qty,
        available_qty: b.available_qty,
        unmeasured_input_count: b.unmeasured_input_count
      };
    };

    // ---- 原料与供应批号：2 条固定查询，按原料归组（不随批号数增长）----
    const materialRows = db.prepare(
      'SELECT id, code, name, allergens FROM materials ORDER BY code'
    ).all();
    const lotRowsByMaterial = new Map(materialRows.map((m) => [m.id, []]));
    db.prepare(
      `SELECT id, material_id, lot_code, supplier, note, status, flagged_reason, created_at
         FROM material_lots ORDER BY material_id, lot_code, id`
    ).all().forEach((l) => {
      if (lotRowsByMaterial.has(l.material_id)) lotRowsByMaterial.get(l.material_id).push(l);
    });
    const materials = materialRows.map((m) => ({
      ...m,
      allergens: JSON.parse(m.allergens),
      lots: (lotRowsByMaterial.get(m.id) || []).map((l) => {
        const { material_id, ...lotFields } = l; // 与原形状一致：批号项不带 material_id
        return {
          ...lotFields,
          flagged: l.status === 'FLAGGED',
          // 该批号当前是否有生效中的质量隔离令（FLAGGED 异常标记仍只作留痕，互不影响）。
          quarantine_active: quarByLot.has(l.id),
          active_quarantine_order: quarByLot.has(l.id) ? quarByLot.get(l.id).order_code : null,
          // 库存计量：期初/入库/领用/可用量及未计量旧输入数（未启用批号 metered=false、可用量 null）。
          ...inventoryBrief(l.id)
        };
      })
    }));

    // ---- 设备（含当前残留快照）----
    const equipment = db.prepare(
      `SELECT e.id, e.code, e.name,
              COALESCE(r.allergens, '[]') AS allergens,
              COALESCE(r.paths_json, '{}') AS paths_json,
              r.last_cleaned_at
         FROM equipment e
         LEFT JOIN equipment_residue r ON r.equipment_id = e.id
        ORDER BY e.code`
    ).all().map((e) => ({
      id: e.id, code: e.code, name: e.name,
      allergens: JSON.parse(e.allergens),
      paths: JSON.parse(e.paths_json),
      last_cleaned_at: e.last_cleaned_at,
      // 最近一次清洗的范围（null/缺省=全清），由下方时间线回放结果回填。
      last_clean_allergens: null,
      last_clean_partial: false
    }));

    // ---- 事件：1 条主查询 + 1 条全部输入（含原料/引用批次/供应批号联表）----
    const eventRows = db.prepare(
      `SELECT ev.id, ev.event_time, ev.type, ev.equipment_id, ev.note,
              ev.clean_allergens,
              ev.batch_id, b.code AS batch_code,
              bo.allergens AS out_allergens,
              bo.paths_json AS out_paths
         FROM events ev
         LEFT JOIN batches b ON b.id = ev.batch_id
         LEFT JOIN batch_outputs bo ON bo.batch_id = ev.batch_id
        ORDER BY ev.event_time, ev.id`
    ).all();

    // 全部事件输入：1 条联表查询（原料/引用批次/供应批号），同时供时间线回放与事件归组。
    const inputRows = db.prepare(
      `SELECT pi.event_id, pi.material_id, pi.batch_id AS ref_batch_id,
              pi.material_lot_id, pi.qty_grams,
              m.name AS material_name, m.code AS material_code,
              m.allergens AS material_allergens,
              rb.code AS ref_batch_code,
              ml.lot_code AS material_lot_code,
              ml.status AS material_lot_status
         FROM process_inputs pi
         LEFT JOIN materials m ON m.id = pi.material_id
         LEFT JOIN batches rb ON rb.id = pi.batch_id
         LEFT JOIN material_lots ml ON ml.id = pi.material_lot_id
        ORDER BY pi.event_id, pi.id`
    ).all();

    // 逐时回放一次（复用本事务已取的原料/批次/设备/事件/输入，得到同一时点的
    // 每次清洗范围、清洗后残留与设备最近一次清洗范围）。回放只读事件/外键三列，
    // 联表后的额外字段不影响其归组。
    const replay = replayTimeline(db, {
      materials: new Map(materialRows.map((m) => [m.id, {
        ...m, allergenList: JSON.parse(m.allergens || '[]')
      }])),
      batches: new Map(eventRows
        .filter((ev) => ev.batch_id != null)
        .map((ev) => [ev.batch_id, { id: ev.batch_id, code: ev.batch_code }])),
      equipment: new Map(equipment.map((q) => [q.id, q])),
      events: eventRows,
      inputs: inputRows
    });

    // 事件 -> 外部记录号（来自断网补录台账；同一事件可能被多个补录包同号复用，取首次）。
    const backfillRefByEvent = new Map();
    db.prepare(
      `SELECT br.event_id, br.external_ref, bb.backfill_code
         FROM backfill_records br JOIN backfill_batches bb ON bb.id = br.backfill_id
        WHERE br.event_id IS NOT NULL ORDER BY br.id`
    ).all().forEach((r) => {
      if (!backfillRefByEvent.has(r.event_id)) {
        backfillRefByEvent.set(r.event_id, {
          external_ref: r.external_ref,
          backfill_code: r.backfill_code
        });
      }
    });
    const cleanInfoByEvent = new Map();
    for (const rev of replay.events) {
      if (rev.type !== 'CLEAN') continue;
      cleanInfoByEvent.set(rev.id, {
        clean_allergens: rev.clean_scope,
        partial: rev.clean_scope !== null,
        post_clean_residue: rev.post_clean_residue
      });
      const q = equipment.find((x) => x.id === rev.equipment_id);
      if (q) {
        q.last_clean_allergens = rev.clean_scope;
        q.last_clean_partial = rev.clean_scope !== null;
      }
    }

    // 全部事件输入已随上面一条查询取出，按 event_id 归组（严格不跨事件/批次/卷混入）。
    const inputsByEvent = new Map();
    inputRows.forEach((r) => {
      if (!inputsByEvent.has(r.event_id)) inputsByEvent.set(r.event_id, []);
      inputsByEvent.get(r.event_id).push({
        material_id: r.material_id,
        ref_batch_id: r.ref_batch_id,
        material_lot_id: r.material_lot_id,
        lot_known: r.material_id != null && r.material_lot_id != null,
        material_code: r.material_code,
        material_name: r.material_name,
        material_allergens: r.material_allergens ? JSON.parse(r.material_allergens) : null,
        material_lot_code: r.material_lot_code,
        material_lot_status: r.material_lot_status,
        // 领用克数：已计量输入为正整数；旧输入/未启用批号/批号未知为 null（不计入领用）。
        qty_grams: r.qty_grams,
        measured: r.material_id != null && r.qty_grams != null,
        ref_batch_code: r.ref_batch_code
      });
    });

    const events = eventRows.map((ev) => ({
      id: ev.id,
      event_time: ev.event_time,
      type: ev.type,
      equipment_id: ev.equipment_id,
      note: ev.note,
      batch_id: ev.batch_id,
      batch_code: ev.batch_code,
      // 若事件来自断网补录，附首次补录该事件的外部记录号与补录批号（只读展示）。
      ...(backfillRefByEvent.get(ev.id) || {}),
      clean_allergens: ev.clean_allergens ? JSON.parse(ev.clean_allergens) : null,
      clean_scope_full: ev.clean_allergens == null,
      ...(ev.type === 'CLEAN' ? {
        clean_partial: ev.clean_allergens != null,
        post_clean_residue: (cleanInfoByEvent.get(ev.id) || {}).post_clean_residue || []
      } : {}),
      out_allergens: ev.out_allergens ? JSON.parse(ev.out_allergens) : [],
      out_paths: ev.out_paths ? JSON.parse(ev.out_paths) : {},
      inputs: inputsByEvent.get(ev.id) || []
    }));

    // ---- 批次：1 条主查询（含产出时间/设备），放行 1 条批量查询后按批归组 ----
    const batchRows = db.prepare(
      `SELECT b.id, b.code, bo.allergens, bo.paths_json,
              ev.event_time AS produced_at, ev.equipment_id,
              eq.code AS equipment_code, eq.name AS equipment_name
         FROM batches b
         LEFT JOIN batch_outputs bo ON bo.batch_id = b.id
         LEFT JOIN events ev ON ev.batch_id = b.id
         LEFT JOIN equipment eq ON eq.id = ev.equipment_id
        ORDER BY ev.event_time, b.code`
    ).all();
    const releasesByBatch = new Map(batchRows.map((b) => [b.id, []]));
    db.prepare(
      `SELECT id, batch_id, released_at, released_by, snapshot_allergens, status, invalid_reason
         FROM releases ORDER BY batch_id, released_at DESC, id DESC`
    ).all().forEach((r) => {
      if (releasesByBatch.has(r.batch_id)) releasesByBatch.get(r.batch_id).push(r);
    });
    const batches = batchRows.map((b) => ({
      id: b.id,
      code: b.code,
      produced_at: b.produced_at,
      equipment_id: b.equipment_id,
      equipment_code: b.equipment_code,
      equipment_name: b.equipment_name,
      allergens: b.allergens ? JSON.parse(b.allergens) : [],
      paths: b.paths_json ? JSON.parse(b.paths_json) : {},
      // 质量隔离令：当前是否受控及覆盖它的生效令（确定命中/批号未知无法排除均受控）。
      quarantined: quarByBatch.has(b.id),
      quarantine_orders: (quarByBatch.get(b.id) || []).map((o) => ({
        order_id: o.order_id,
        order_code: o.order_code,
        lot_id: o.lot_id,
        lot_code: o.lot_code,
        material_code: o.material_code,
        material_name: o.material_name,
        classification: o.classification,
        reason: o.reason
      })),
      releases: (releasesByBatch.get(b.id) || []).map((r) => {
        const { batch_id, ...rest } = r; // 与原形状一致：release 项不带 batch_id
        return {
          ...rest,
          snapshot_allergens: JSON.parse(r.snapshot_allergens)
        };
      })
    }));

    // ---- 标签卷：1 条卷查询 + 1 条分配汇总 + 2 条核销/更正链批量查询 ----
    const rollRows = db.prepare(
      `SELECT lr.*, b.code AS batch_code,
              r.released_at AS release_at, r.released_by AS release_by
         FROM label_rolls lr
         LEFT JOIN batches b ON b.id = lr.batch_id
         LEFT JOIN releases r ON r.id = lr.release_id
        ORDER BY b.code, lr.roll_code`
    ).all();
    const allocatedByRoll = new Map(
      db.prepare(
        `SELECT roll_id, COALESCE(SUM(qty), 0) AS qty
           FROM shipment_items GROUP BY roll_id`
      ).all().map((r) => [r.roll_id, r.qty || 0])
    );
    const usagesByRoll = labels.listUsagesForRolls(db, rollRows.map((r) => r.id));
    const label_rolls = rollRows.map((r) => ({
      ...labels.formatRoll(r),
      batch_code: r.batch_code,
      release_at: r.release_at,
      release_by: r.release_by,
      allocated_qty: allocatedByRoll.get(r.id) || 0,
      // 每笔核销流水：原始数量、最新有效数量与更正链（更正只增不改，累计按最新有效数量计）。
      usages: usagesByRoll.get(r.id) || [],
      // 可出货余额：仅未停用卷的“已贴用 - 累计已分配”；停卷余量冻结不可出货。
      // 质量隔离令：所属批次被任一生效令覆盖时逐卷可出货量按 0（与余额接口/出货入口一致），
      // 已贴用数、已分配数与卷状态均保留原值。
      shippable_qty: r.status === 'ACTIVE' && !quarByBatch.has(r.batch_id)
        ? Math.max(r.applied_qty - (allocatedByRoll.get(r.id) || 0), 0)
        : 0,
      // 质量隔离令：所属批次受控时暂停贴用（不永久停用；脱离全部生效令后自动恢复）。
      quarantine_paused: r.status === 'ACTIVE' && quarByBatch.has(r.batch_id)
    }));

    // ---- 出货单：1 条主查询；明细与处置各 1 条批量查询，受控批次集合复用本事务快照 ----
    const shipment_rows = db.prepare('SELECT * FROM shipments ORDER BY id DESC').all();
    const shipments_list = shipments.formatShipmentsBulk(db, shipment_rows, {
      controlledBatchIds
    });

    // 设备检修停机窗口（含各窗口当前覆盖的冲突事件，正常恒为空：两侧写入均拒绝冲突）。
    // 复用本事务已取的事件行在内存过滤，不再逐窗口查询。
    const downtime_windows = downtime.listWindows(db, { allEvents: eventRows });

    // 质量隔离令台账（生效令含当前受控批次清单；传播依据见单条查询接口；范围批量取数）。
    const quarantine_orders = quarantine.listOrders(db);

    // 批号库存台账（各批号期初/入库/领用/可用量及未计量旧输入数）。
    const lot_inventory = inventoryData.lots;

    return {
      materials, equipment, events, batches, label_rolls,
      shipments: shipments_list, downtime_windows, quarantine_orders, lot_inventory,
      lot_inventory_summary: inventoryData.summary
    };
  })();
}

module.exports = { getPageState };
