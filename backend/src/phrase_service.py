import re,redis
from fastapi import HTTPException
from sqlalchemy import select,func
from .models import PhraseRule,Triage,Intake,now
from .common import get,reject,finish,dump
from .triage_service import queue
PUNCT=re.compile(r'[\s，。、；：？！…—·“”‘’「」『』《》〈〉（）【】,.;:?!()\-—\[\]<>]+')
def strip_text(text,phrases):
    out=text
    for p in phrases:
        if p:out=out.replace(p,' ')
    return ' '.join(out.split())
def remaining(text,phrases):return PUNCT.sub('',strip_text(text,phrases))
def active_rule(db):return db.scalar(select(PhraseRule).where(PhraseRule.status=='active'))
def create(db,data,user):
    phrases=[]
    for p in data.phrases:
        p=' '.join(p.split())
        if p and p not in phrases:phrases.append(p)
    reject(not phrases,'套话条目不能为空')
    reject(any(len(p)<2 or len(p)>60 for p in phrases),'每条套话长度需2–60字')
    version=(db.scalar(select(func.max(PhraseRule.version)))or 0)+1
    row=PhraseRule(version=version,phrases=phrases,note=data.note,actor=user.username);db.add(row);db.flush()
    return finish(db,user,'登记套话规则v%d'%version,row)
def activate(db,id,user):
    row=get(db,PhraseRule,id,True)
    if row.status=='active':return dump(row)  # 重试启用幂等，不重复派发重算
    reject(row.status!='draft','仅待启用版本可启用')
    current=db.scalar(select(PhraseRule).where(PhraseRule.status=='active').with_for_update())
    if current:current.status='archived'
    row.status='active';row.activated_at=now()
    # 未复核旧建议失效；已复核结论照旧。排队/识别中的卷宗按新规则重算。
    stale=db.scalars(select(Triage).where(Triage.status.in_(['queued','running','done'])).with_for_update().execution_options(populate_existing=True)).all()
    inflight={}
    for t in stale:
        if t.status in('queued','running') and t.intake_id not in inflight:inflight[t.intake_id]=True
        t.status='invalid';t.error='套话规则v%d启用，未复核建议失效'%row.version
    recalc=[]
    for intake_id in inflight:
        case=db.get(Intake,intake_id)
        if not case or case.status=='closed':continue
        fresh=Triage(intake_id=case.id,facts_version=case.facts_version);db.add(fresh);recalc.append(fresh)
    db.flush();result=finish(db,user,'启用套话规则v%d'%row.version,row)
    pushed=0
    try:
        for t in recalc:queue.rpush('aid:triage',t.id);pushed+=1
    except redis.RedisError:
        for t in recalc[pushed:]:t.status='failed';t.error='任务队列暂不可用，恢复后可重新提交'
        db.commit()
        raise HTTPException(503,'任务队列暂不可用，规则已启用，恢复后可重新提交分流')
    return result
