// Скільки займає пачка рядків після gzip (gRPC стискає кожне повідомлення окремо).
// Рядок закодовано приблизно як protobuf: GUID-и по 16 байт, varint-и, рядки UTF-8.
const zlib=require('zlib');const crypto=require('crypto');
let seed=7;const rnd=()=>{seed=(seed*1103515245+12345)&0x7fffffff;return seed/0x7fffffff};
const words=['Степлер','Папір','Маркери','Палета','Плівка','Скотч','Принтер','Кабель','Лампа','Монітор','Ящик','Фарба','Стілець','Коробка','Етикетки','Батарейки'];
const cats=Array.from({length:20},()=>crypto.randomBytes(16));
function varint(n){const b=[];do{let x=n&127;n=Math.floor(n/128);if(n)x|=128;b.push(x)}while(n);return Buffer.from(b)}
function str(s){const b=Buffer.from(s,'utf8');return Buffer.concat([varint(b.length),b])}
let ver=1_000_000,t=1_790_000_000_000;
function row(kind){
  ver++;t+=Math.floor(rnd()*200);
  const id=Buffer.alloc(16);crypto.randomFillSync(id);id.writeUInt32BE(Math.floor(t/1000),0); // GUID v7: префікс часу
  const parts=[id,cats[Math.floor(rnd()*cats.length)],varint(ver),varint(Math.floor(rnd()*100000)),varint(Math.floor(rnd()*5)),
    Buffer.alloc(8,0),varint(t),str(words[Math.floor(rnd()*words.length)]+' '+Math.floor(rnd()*1000)),
    str(['новий','активний','архів'][Math.floor(rnd()*3)])];
  if(kind==='wide')parts.push(str(Array.from({length:12},()=>words[Math.floor(rnd()*words.length)]).join(' ')));
  return Buffer.concat(parts);
}
const out=[];
for(const kind of ['narrow','wide'])for(const n of [1,3,10,50,200,1000,5000]){
  const rows=Array.from({length:n},()=>row(kind));const raw=Buffer.concat(rows);const gz=zlib.gzipSync(raw);
  out.push({kind,n,rawPerRow:+(raw.length/n).toFixed(1),gzPerRow:+((gz.length+5)/n).toFixed(1),ratio:+(raw.length/gz.length).toFixed(2)});
}
console.table(out);require('fs').writeFileSync(__dirname+'/gzip-bench.json',JSON.stringify(out,null,1));
