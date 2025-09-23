
import React,{useEffect,useState} from 'react'
export default function HomePage(){
  const [daily,setDaily]=useState<any[]>([]); const [perCam,setPerCam]=useState<any>({})
  useEffect(()=>{ fetch('/api/metrics/daily?days=7').then(r=>r.json()).then(setDaily)
                  fetch('/api/metrics/daily_by_camera?days=7').then(r=>r.json()).then(setPerCam)},[])
  return (<div style={{padding:24}}><h2>Dashboard</h2><pre>{JSON.stringify(daily,null,2)}</pre>
  <h3>Per-camera footfall & zones</h3><pre>{JSON.stringify(perCam,null,2)}</pre></div>)
}
