from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from notify_flow.db.models import Member, Notification, ProviderDelivery


async def add_member(sm, member_id="m-001", **kw):
    values = dict(
        id=member_id,
        first_name="Priya",
        email=f"{member_id}@example.test",
        phone="+15550100",
        timezone="UTC",
        program_week=3,
        sessions_last_7d=1,
        days_since_last_session=8,
        open_rate_push=0.8,
        open_rate_sms=0.4,
        open_rate_email=0.1,
    )
    values.update(kw)
    async with sm.begin() as s:
        await s.execute(insert(Member).values(**values))


async def count(sm, model, **where):
    async with sm() as s:
        q = select(func.count()).select_from(model)
        for k, v in where.items():
            q = q.where(getattr(model, k) == v)
        return (await s.execute(q)).scalar_one()


async def notifications(sm, member_id):
    async with sm() as s:
        return (
            (
                await s.execute(
                    select(Notification)
                    .where(Notification.member_id == member_id)
                    .order_by(Notification.step)
                )
            )
            .scalars()
            .all()
        )


async def deliveries(sm, member_id):
    return await count(sm, ProviderDelivery, member_id=member_id)
