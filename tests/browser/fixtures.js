// Test-only replacements loaded before the unchanged production dashboard script.
window.fixture={
 conference:{conference_id:'browser-run-1',mode:'codex',status:'running',stage:'Independent research and build',objective:'Synthetic browser regression task',provider:'Browser test fixture',bridge_protocol_version:'1.1',participants:['Researcher','Builder','Reviewer','Moderator'].map(participant_id=>({participant_id,state:'working'}))},
 utterances:[],cancellations:0,source:null,
};
window.fetch=async()=>({ok:true,json:async()=>({conference:structuredClone(fixture.conference)})});
window.setInterval=()=>0;
window.EventSource=class{constructor(){fixture.source=this}};
window.SpeechSynthesisUtterance=class{constructor(text){this.text=text}};
Object.defineProperty(window,'speechSynthesis',{configurable:true,value:{
 getVoices:()=>['Voice A','Voice B','Voice C','Voice D'].map(name=>({name})),
 speak:utterance=>fixture.utterances.push(utterance),
 cancel:()=>fixture.cancellations++,pause:()=>{},resume:()=>{},
}});
