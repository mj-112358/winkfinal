
import React,{useEffect,useState} from 'react'
export default function ZonesPage(){
  const [cams,setCams]=useState<any[]>([]); const [cam,setCam]=useState<number>(0)
  const [list,setList]=useState<any>({screenshot:null,zones:[]}); const [file,setFile]=useState<File|null>(null)
  const [imgW,setImgW]=useState(1280); const [imgH,setImgH]=useState(720); const [name,setName]=useState('')
  const [ztype,setZtype]=useState('zone'); const [poly,setPoly]=useState('[[100,100],[200,100],[200,200],[100,200]]')
  const loadCams=()=>fetch('/api/cameras').then(r=>r.json()).then(setCams)
  const load=(id:number)=>fetch('/api/zones?camera_id='+id).then(r=>r.json()).then(setList)
  useEffect(()=>{loadCams()},[]); useEffect(()=>{if(cam) load(cam)},[cam])
  const upload=async()=>{ if(!file||!cam) return; const fd=new FormData(); fd.append('camera_id',String(cam)); fd.append('file',file);
    fd.append('img_width',String(imgW)); fd.append('img_height',String(imgH));
    await fetch('/api/zones/screenshot',{method:'POST',body:fd}); load(cam) }
  const add=async()=>{ const fd=new FormData(); fd.append('camera_id',String(cam)); fd.append('name',name); fd.append('ztype',ztype); fd.append('polygon_json',poly);
    await fetch('/api/zones',{method:'POST',body:fd}); setName(''); setPoly(''); load(cam) }
  const del=async(id:number)=>{ await fetch('/api/zones/'+id,{method:'DELETE'}); load(cam) }
  return (<div style={{padding:24}}><h2>Zones & Shelves</h2>
  <div style={{display:'flex',gap:8,alignItems:'end'}}>
    <select value={cam} onChange={e=>setCam(parseInt(e.target.value||'0'))}>
      <option value={0}>Select camera...</option>{cams.map((c:any)=>(<option key={c.id} value={c.id}>{c.name}</option>))}
    </select>
    <input type='file' onChange={e=>setFile(e.target.files?.[0]||null)}/>
    <input type='number' value={imgW} onChange={e=>setImgW(parseInt(e.target.value||'1280'))}/>
    <input type='number' value={imgH} onChange={e=>setImgH(parseInt(e.target.value||'720'))}/>
    <button onClick={upload}>Upload Screenshot</button>
    {cam ? <a href={'/api/zones/overlay?camera_id='+cam} target='_blank'>Preview Overlay</a> : null}
  </div>
  <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16,marginTop:16}}>
    <div><h3>Add</h3>
      <input placeholder='Name' value={name} onChange={e=>setName(e.target.value)}/><br/>
      <select value={ztype} onChange={e=>setZtype(e.target.value)}><option value='zone'>Zone</option><option value='shelf'>Shelf</option><option value='queue'>Queue</option><option value='entry'>Entry</option></select><br/>
      <textarea value={poly} onChange={e=>setPoly(e.target.value)} style={{width:'100%',height:160}} placeholder='[[x,y],[x,y],...]'/><br/>
      <button onClick={add}>Add Zone</button>
    </div>
    <div><h3>Existing</h3>
      <table><tbody>{list.zones?.map((z:any)=>(<tr key={z.id}><td>{z.name}</td><td>{z.ztype}</td><td><button onClick={()=>del(z.id)}>Delete</button></td></tr>))}</tbody></table>
    </div>
  </div></div>)
}
