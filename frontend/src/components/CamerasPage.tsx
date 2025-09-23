
import React,{useEffect,useState} from 'react'
export default function CamerasPage(){
  const [cams,setCams]=useState<any[]>([]); const [name,setName]=useState(''); const [rtsp,setRtsp]=useState('')
  const load=()=>fetch('/api/cameras').then(r=>r.json()).then(setCams)
  useEffect(()=>{load()},[])
  const add=async()=>{await fetch('/api/cameras',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,rtsp_url:rtsp,enabled:true})}); setName(''); setRtsp(''); load()}
  const del=async(id:number)=>{await fetch('/api/cameras/'+id,{method:'DELETE'}); load()}
  return (<div style={{padding:24}}><h2>Cameras</h2>
  <div style={{display:'flex',gap:8}}><input placeholder='Name' value={name} onChange={e=>setName(e.target.value)}/>
  <input placeholder='rtsp://...' style={{width:500}} value={rtsp} onChange={e=>setRtsp(e.target.value)}/>
  <button onClick={add}>Add</button></div>
  <table><tbody>{cams.map(c=>(<tr key={c.id}><td>{c.name}</td><td>{c.rtsp_url}</td><td><button onClick={()=>del(c.id)}>Delete</button></td></tr>))}</tbody></table>
  </div>)
}
