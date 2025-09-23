
import React,{useState} from 'react'
export default function InsightsPage(){
  const [weeks,setWeeks]=useState(1); const [promo,setPromo]=useState(false)
  const [ps,setPs]=useState(''); const [pe,setPe]=useState(''); const [fest,setFest]=useState(false)
  const [fs,setFs]=useState(''); const [fe,setFe]=useState(''); const [out,setOut]=useState('')
  const go=async()=>{
    const body:any={period_weeks:weeks,promo_enabled:promo,promo_start:promo?ps:null,promo_end:promo?pe:null,festival_enabled:fest,festival_start:fest?fs:null,festival_end:fest?fe:null}
    const r=await fetch('/api/insights/combined',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); setOut(JSON.stringify(await r.json(),null,2))
  }
  return (<div style={{padding:24}}><h2>AI Insights</h2>
  <div style={{display:'flex',gap:12,alignItems:'end'}}>
    <div><label>Weeks</label><br/><input type='number' value={weeks} onChange={e=>setWeeks(parseInt(e.target.value||'1'))} style={{width:80}}/></div>
    <label><input type='checkbox' checked={promo} onChange={e=>setPromo(e.target.checked)}/> Promo effectiveness</label>
    {promo && (<><input type='date' value={ps} onChange={e=>setPs(e.target.value)}/><input type='date' value={pe} onChange={e=>setPe(e.target.value)}/></>)}
    <label><input type='checkbox' checked={fest} onChange={e=>setFest(e.target.checked)}/> Festival spike</label>
    {fest && (<><input type='date' value={fs} onChange={e=>setFs(e.target.value)}/><input type='date' value={fe} onChange={e=>setFe(e.target.value)}/></>)}
    <button onClick={go}>Generate</button>
  </div>
  <pre style={{whiteSpace:'pre-wrap',background:'#111',color:'#eee',padding:16,marginTop:16}}>{out}</pre>
  </div>)
}
