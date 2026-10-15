'use strict';
// One-time, opt-in repair. Never runs during application startup.
// Usage: DATABASE_URL=... node scripts/repair-dropouts-2026-10-15.js --dry-run
//        DATABASE_URL=... node scripts/repair-dropouts-2026-10-15.js --apply
const { Pool } = require('pg');
const crypto = require('crypto');
const pool = new Pool({connectionString:process.env.DATABASE_URL,ssl:process.env.PGSSLMODE==='disable'?false:{rejectUnauthorized:false}});
const start='2026-10-14T20:20:00Z', end='2026-10-15T02:20:00Z';
const specs={
 trend_history:['planks_in','planks_out','ambient','planks_concrete','planks_tank','ambient_concrete_diff'],
 tbeams_trend_history:['tbeams_in','tbeams_out','ambient','tbeams_concrete','tbeams_tank','ambient_concrete_diff']
};
async function run(){
 const apply=process.argv.includes('--apply');
 const client=await pool.connect();
 try {
  await client.query('BEGIN');
  await client.query('SELECT pg_advisory_xact_lock(20261015, 650)');
  const summary=[];
  for(const [table,cols] of Object.entries(specs)){
   const {rows}=await client.query(`SELECT id,recorded_at,${cols.join(',')} FROM ${table} WHERE recorded_at >= $1 AND recorded_at <= $2 ORDER BY recorded_at,id FOR UPDATE`,[start,end]);
   const edits=[];
   for(const col of cols){
    // Only interpolate short, isolated downward dropouts bounded by plausible readings.
    for(let i=1;i<rows.length-1;i++){
     const v=rows[i][col],before=rows[i-1][col];
     if(!Number.isFinite(v)||!Number.isFinite(before)||v>=before-12)continue;
     let j=i;
     while(j<rows.length-1&&j-i<5&&Number.isFinite(rows[j][col])&&rows[j][col]<before-12)j++;
     const after=rows[j]?.[col];
     const gap=(new Date(rows[j]?.recorded_at)-new Date(rows[i-1].recorded_at))/60000;
     if(j===i||j>=rows.length||!Number.isFinite(after)||Math.abs(after-before)>12||gap>8)continue;
     for(let k=i;k<j;k++){
      const fraction=(new Date(rows[k].recorded_at)-new Date(rows[i-1].recorded_at))/(new Date(rows[j].recorded_at)-new Date(rows[i-1].recorded_at));
      const corrected=Math.round((before+(after-before)*fraction)*1000)/1000;
      edits.push({id:rows[k].id,col,original:rows[k][col],corrected});
     }
     i=j-1;
    }
   }
   summary.push({table,rows:rows.length,edits:edits.length,preview:edits.slice(0,15)});
   if(apply&&edits.length){
    await client.query(`CREATE TABLE IF NOT EXISTS trend_dropout_repair_backup_20261015 (table_name text NOT NULL,row_id bigint NOT NULL,column_name text NOT NULL,original_value double precision,corrected_value double precision,recorded_at timestamptz NOT NULL,backed_up_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(table_name,row_id,column_name))`);
    for(const e of edits){
     const row=rows.find(r=>r.id===e.id);
     await client.query('INSERT INTO trend_dropout_repair_backup_20261015(table_name,row_id,column_name,original_value,corrected_value,recorded_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',[table,e.id,e.col,e.original,e.corrected,row.recorded_at]);
     await client.query(`UPDATE ${table} SET ${e.col}=$1 WHERE id=$2 AND ${e.col} IS NOT DISTINCT FROM $3`,[e.corrected,e.id,e.original]);
    }
   }
  }
  console.log(JSON.stringify({mode:apply?'apply':'dry-run',window:{start,end},summary},null,2));
  if(apply)await client.query('COMMIT');else await client.query('ROLLBACK');
 }catch(e){await client.query('ROLLBACK');throw e}finally{client.release();await pool.end()}
}
run().catch(e=>{console.error(e);process.exitCode=1});
