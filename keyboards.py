from asyncio.log import logger
from datetime import datetime, timedelta
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func, and_, exists
from sqlalchemy.ext.asyncio import AsyncSession

from database import SessionLocal
from models import Service, Schedule, Booking, User

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="👁️ Просмотр записей"), KeyboardButton(text="📅 Создать расписание")],
        [KeyboardButton(text="📝 Добавить услугу"), KeyboardButton(text="✏️ Редактировать услугу")],
        [KeyboardButton(text="🗑️ Удалить услугу"), KeyboardButton(text="📢 Рассылка")],
        [KeyboardButton(text="📋 Посмотреть расписание"), KeyboardButton(text="📝 Посмотреть отзывы")]  # Новая кнопка
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_client_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="💈 Записаться на услугу"), KeyboardButton(text="📋 Мои записи")],
        [KeyboardButton(text="🔄 Перенести запись"), KeyboardButton(text="❌ Отменить запись")],
        [KeyboardButton(text="📝 Оставить отзыв")]  # Важно: текст должен совпадать с обработчиком
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def get_confirm_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_{booking_id}"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_{booking_id}")
        ]
    ])

async def get_services_keyboard() -> ReplyKeyboardMarkup:
    async with SessionLocal() as session:
        services = await session.execute(select(Service))
        buttons = [[KeyboardButton(text=f"{service.name} - {service.price}₽")] for service in services.scalars()]
        buttons.append([KeyboardButton(text="🔙 Назад")])
        return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

async def get_days_keyboard_for_month(month: str) -> ReplyKeyboardMarkup:
    async with SessionLocal() as session:
        try:
            # Русские названия месяцев для обратного преобразования
            month_map = {
                'Январь': 1, 'Февраль': 2, 'Март': 3,
                'Апрель': 4, 'Май': 5, 'Июнь': 6,
                'Июль': 7, 'Август': 8, 'Сентябрь': 9,
                'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
            }
            
            # Разбираем строку месяца (формат "Апрель 2025")
            month_parts = month.split()
            if len(month_parts) != 2:
                raise ValueError("Неверный формат месяца")
                
            month_name = month_parts[0]
            year = int(month_parts[1])
            month_num = month_map.get(month_name)
            
            if not month_num:
                raise ValueError(f"Неизвестный месяц: {month_name}")

            # Границы месяца
            start_date = datetime(year, month_num, 1)
            if month_num == 12:
                end_date = datetime(year + 1, 1, 1)
            else:
                end_date = datetime(year, month_num + 1, 1)

            # Получаем все дни месяца, где есть хотя бы один слот
            days = await session.execute(
                select(func.to_char(Schedule.date, 'DD.MM.YYYY').label("day"))
                .where(
                    Schedule.date >= start_date,
                    Schedule.date < end_date,
                    Schedule.date >= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                )
                .distinct()
                .order_by("day")
            )

            buttons = [[KeyboardButton(text=day)] for day in days.scalars()]
            if not buttons:
                buttons.append([KeyboardButton(text="Нет доступных дат")])
            buttons.append([KeyboardButton(text="🔙 Назад")])
            
            return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
            
        except Exception as e:
            logger.error(f"Error in get_days_keyboard_for_month: {str(e)}")
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Ошибка загрузки дат")],
                    [KeyboardButton(text="🔙 Назад")]
                ],
                resize_keyboard=True
            )
        
async def get_months_keyboard(admin_mode=False):
    async with SessionLocal() as session:
        month_translation = {
            'January': 'Январь', 'February': 'Февраль', 'March': 'Март',
            'April': 'Апрель', 'May': 'Май', 'June': 'Июнь',
            'July': 'Июль', 'August': 'Август', 'September': 'Сентябрь',
            'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'
        }
        
        months = await session.execute(
            select(
                func.to_char(Schedule.date, 'Month YYYY').label("month"),
                func.to_char(Schedule.date, 'MM.YYYY').label("month_key")
            )
            .where(Schedule.date >= datetime.now())
            .group_by("month", "month_key")
            .order_by(func.min(Schedule.date))
        )

        buttons = []
        for month, month_key in months:
            eng_month = month.split()[0]
            ru_month = month_translation.get(eng_month, eng_month)
            ru_month_str = f"{ru_month} {month.split()[1]}"
            buttons.append([KeyboardButton(text=ru_month_str.strip())])
        
        # Всегда добавляем кнопку "Назад"
        buttons.append([KeyboardButton(text="🔙 Назад")])
        
        return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

async def get_days_keyboard_for_month(month: str, admin_mode=False):
    async with SessionLocal() as session:
        try:
            month_map = {
                'Январь': 1, 'Февраль': 2, 'Март': 3,
                'Апрель': 4, 'Май': 5, 'Июнь': 6,
                'Июль': 7, 'Август': 8, 'Сентябрь': 9,
                'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
            }
            
            month_parts = month.split()
            month_name = month_parts[0]
            year = int(month_parts[1])
            month_num = month_map.get(month_name)
            
            start_date = datetime(year, month_num, 1)
            if month_num == 12:
                end_date = datetime(year + 1, 1, 1)
            else:
                end_date = datetime(year, month_num + 1, 1)

            days = await session.execute(
                select(func.to_char(Schedule.date, 'DD.MM.YYYY').label("day"))
                .where(
                    Schedule.date >= start_date,
                    Schedule.date < end_date,
                    Schedule.date >= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                )
                .distinct()
                .order_by("day")
            )

            buttons = [[KeyboardButton(text=day)] for day in days.scalars()]
            
            # Всегда добавляем кнопку "Назад"
            buttons.append([KeyboardButton(text="🔙 Назад")])
            
            return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
            
        except Exception as e:
            logger.error(f"Error in get_days_keyboard_for_month: {str(e)}")
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Ошибка загрузки дат")],
                    [KeyboardButton(text="🔙 Назад")]
                ],
                resize_keyboard=True
            )

async def get_times_keyboard(day: str, exclude_booking_id: int = None):
    try:
        day_date = datetime.strptime(day, '%d.%m.%Y').date()
    except ValueError:
        return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Назад")]], resize_keyboard=True)
    
    async with SessionLocal() as session:
        # Получаем только свободные слоты
        query = select(Schedule).where(
            func.date(Schedule.date) == day_date,
            Schedule.date >= datetime.now(),
            ~exists().where(
                and_(
                    Booking.schedule_id == Schedule.id,
                    Booking.confirmed == True
                )
            )
        )
        
        if exclude_booking_id:
            query = query.where(
                ~exists().where(
                    and_(
                        Booking.schedule_id == Schedule.id,
                        Booking.confirmed == True,
                        Booking.id != exclude_booking_id
                    )
                )
            )
        
        available_slots = await session.execute(query.order_by(Schedule.date))
        
        buttons = [
            [KeyboardButton(text=slot.date.strftime('%H:%M'))]
            for slot in available_slots.scalars()
        ]
        
        if not buttons:
            buttons.append([KeyboardButton(text="Нет свободных слотов")])
        
        buttons.append([KeyboardButton(text="🔙 Назад")])
        return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
async def get_user_bookings_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    async with SessionLocal() as session:
        user = await session.execute(
            select(User).where(User.telegram_id == str(user_id))
        )
        user = user.scalars().first()
        
        if not user:
            return None
        
        bookings = await session.execute(
            select(Booking, Service)
            .join(Service)
            .where(
                Booking.user_id == user.id,
                Booking.date >= datetime.now()
            )
            .order_by(Booking.date)
        )
        
        buttons = []
        for booking, service in bookings:
            buttons.append([KeyboardButton(
                text=f"{booking.id}: {service.name} на {booking.date.strftime('%d.%m.%Y %H:%M 🕒')}"
            )])
        
        if not buttons:
            return None
            
        buttons.append([KeyboardButton(text="🔙 Назад")])
        return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)