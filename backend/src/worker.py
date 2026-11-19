import logging,time,json
from pathlib import Path
from sqlalchemy import select,update
from .database import Session
from .models import Triage,Intake,PhraseRule
from .triage_service import queue
from .algorithm import classify
from .phrase_service import strip_text,remaining
logging.basicConfig(level=logging.INFO)
def process(db,tid):
    claimed=db.execute(update(Triage).where(Triage.id==tid,Triage.status=='queued').values(status='running').execution_options(synchronize_session=False)).rowcount
    db.commit()
    if not claimed:return
    try:
        row=db.get(Triage,tid);case=db.get(Intake,row.intake_id)
        if case.facts_version!=row.facts_version:raise ValueError('事实版本已更新，请重新提交')
        rule=db.scalar(select(PhraseRule).where(PhraseRule.status=='active'))
        phrases=rule.phrases if rule else[]
        predicted,confidence,metrics=classify(case.facts)
        reasons=['原文置信度低于68%，退回人工']if confidence<.68 else[]
        cleaned=strip_text(case.facts,phrases);sp=sc=None
        if rule:
            if not remaining(case.facts,phrases):reasons.append('去除已登记套话后只剩通用提醒，转人工分流')
            else:
                sp,sc,_=classify(cleaned)
                if sp!=predicted:reasons.append('去除套话后推荐主题变化，转人工核对')
        values=dict(predicted=predicted,confidence=confidence,stripped_predicted=sp,stripped_confidence=sc,manual_required=bool(reasons),manual_reason='；'.join(reasons),rule_version=rule.version if rule else None,metrics=metrics,status='done')
        done=db.execute(update(Triage).where(Triage.id==tid,Triage.status=='running').values(**values).execution_options(synchronize_session=False)).rowcount
        db.commit()
        if done:
            try:Path('/artifacts').mkdir(exist_ok=True);Path(f'/artifacts/triage-{tid}.json').write_text(json.dumps(metrics,ensure_ascii=False))
            except OSError:logging.exception('triage %s 指标留档失败',tid)
        else:logging.info('triage %s 计算期间套话规则已更新，旧结果丢弃',tid)
    except Exception as e:
        db.execute(update(Triage).where(Triage.id==tid,Triage.status=='running').values(status='failed',error=str(e)[:200]).execution_options(synchronize_session=False));db.commit()
def main():
    while True:
        Path('/tmp/worker-heartbeat').touch()
        try:
            item=queue.blpop('aid:triage',timeout=2)
            if not item:continue
            with Session() as db:process(db,int(item[1]))
        except Exception:logging.exception('triage loop');time.sleep(1)
if __name__=='__main__':main()
