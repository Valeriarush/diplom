import asyncio
import re
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from aiogram import Bot, types
from aiogram.fsm.context import FSMContext
from sqlalchemy import select, func, and_, exists, delete
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from database import SessionLocal
from models import Feedback, User, Service, Schedule, Booking
from states import (
    CreateScheduleStates, 
    AddServiceStates, 
    EditServiceStates,
    DeleteServiceStates, 
    AdminStates,
    ViewBookingsStates
)
from keyboards import (
    get_admin_keyboard, 
    get_cancel_keyboard,
    get_days_keyboard_for_month,
    get_months_keyboard, 
    get_services_keyboard,
    get_client_keyboard, 
    get_confirm_keyboard
)
from config import ADMIN_ID

logger = logging.getLogger(__name__)

# Вспомогательные функции
def parse_time_slot(time_str: str) -> Optional[Tuple[int, int]]:
    try:
        hour, minute = map(int, time_str.split(':'))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
        return None
    except ValueError:
        return None

def parse_date_part(date_str: str) -> Optional[Tuple[int, int, int]]:
    parts = date_str.split('.')
    current_year = datetime.now().year
    
    try:
        if len(parts) == 2:  # ДД.ММ
            day, month = map(int, parts)
            year = current_year
        elif len(parts) == 3:  # ДД.ММ.ГГ или ДД.ММ.ГГГГ
            day, month = map(int, parts[:2])
            year = int(parts[2])
            if year < 100:  # Двухзначный год
                year += 2000 if year < 50 else 1900
        else:
            return None
        
        datetime(year=year, month=month, day=day)
        return day, month, year
    except (ValueError, IndexError):
        return None

def is_valid_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except ValueError:
        return False

# Обработчики администратора
async def view_bookings_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return
    
    await message.answer(
        "Выберите месяц для просмотра записей:",
        reply_markup=await get_months_keyboard(admin_mode=True)
    )
    await state.set_state(ViewBookingsStates.waiting_for_month)

async def view_bookings_select_month(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.clear()
        await message.answer("Главное меню", reply_markup=get_admin_keyboard())
        return
    
    await state.update_data(month=message.text)
    await message.answer(
        "Выберите день для просмотра записей:",
        reply_markup=await get_days_keyboard_for_month(message.text, admin_mode=True)
    )
    await state.set_state(ViewBookingsStates.waiting_for_day)

async def view_bookings_select_day(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        # Возвращаемся к выбору месяца
        data = await state.get_data()
        await message.answer(
            "Выберите месяц для просмотра записей:",
            reply_markup=await get_months_keyboard(admin_mode=True)
        )
        await state.set_state(ViewBookingsStates.waiting_for_month)
        return
    
    try:
        day_date = datetime.strptime(message.text, '%d.%m.%Y').date()
    except ValueError:
        await message.answer("Некорректный формат даты. Выберите день из списка.")
        return
    
    async with SessionLocal() as session:
        bookings = await session.execute(
            select(Booking, User, Service)
            .join(User)
            .join(Service)
            .where(func.date(Booking.date) == day_date)
            .order_by(Booking.date)
        )
        
        bookings = bookings.all()
        
        response = f"📅 Записи на {day_date.strftime('%d.%m.%Y')}:\n\n"
        if not bookings:
            response = f"На {day_date.strftime('%d.%m.%Y')} нет записей."
        
        for booking, user, service in bookings:
            response += (
                f"⏰ Время: {booking.date.strftime('%H:%M')}\n"
                f"👤 Клиент: {user.first_name} {user.last_name}\n"
                f"📱 Телефон: {user.phone}\n"
                f"💈 Услуга: {service.name} ({service.price}₽)\n"
                f"Статус: {'✅ Подтверждена' if booking.confirmed else '🕒 Ожидает подтверждения'}\n"
                f"ID записи: {booking.id}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
            )
        
        # Клавиатура только с кнопкой "Назад"
        back_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
        
        await message.answer(response, reply_markup=back_keyboard)

async def create_schedule_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return
    
    await message.answer(
        "Введите даты и времена для создания расписания в формате:\n"
        "02.04 10:00 12:00 14:00 16:00 18:00\n"
        "03.04 10:00 12:00 14:00\n\n"
        "Можно указывать даты в формате ДД.ММ или ДД.ММ.ГГГГ",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(CreateScheduleStates.waiting_for_dates)

async def create_schedule_process(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=get_admin_keyboard())
        return
    
    parts = [p.strip() for p in message.text.split() if p.strip()]
    current_date = None
    created_slots = 0
    errors = []
    
    async with SessionLocal() as session:
        for part in parts:
            # Убедитесь, что даты парсятся правильно
            date_parts = parse_date_part(part)
            if date_parts:
                day, month, year = date_parts
                try:
                    # Корректировка года для двузначных значений
                    if year < 100:
                        current_century = datetime.now().year // 100 * 100
                        year += current_century
                    current_date = datetime(int(year), int(month), int(day))
                    continue
                except ValueError as e:
                    errors.append(f"Некорректная дата: {part} ({str(e)})")
                    continue
            
            time_parts = parse_time_slot(part)
            if time_parts:
                if current_date is None:
                    errors.append(f"Сначала укажите дату перед временем: {part}")
                    continue
                
                hour, minute = time_parts
                try:
                    slot_datetime = current_date.replace(hour=hour, minute=minute)
                    
                    if slot_datetime < datetime.now():
                        errors.append(f"Время уже прошло: {part}")
                        continue
                    
                    existing = await session.execute(
                        select(Schedule).where(Schedule.date == slot_datetime)
                    )
                    if not existing.scalar():
                        session.add(Schedule(date=slot_datetime))
                        created_slots += 1
                except ValueError as e:
                    errors.append(f"Ошибка времени {part}: {str(e)}")
                    continue
            else:
                errors.append(f"Неизвестный формат: {part}")
                continue
        
        await session.commit()
    
    response = []
    if created_slots > 0:
        response.append(f"✅ Добавлено {created_slots} новых слотов расписания")
    else:
        response.append("⚠ Не было добавлено новых слотов")
    
    if errors:
        response.append("\nОшибки обработки:")
        response.extend(errors[:5])
        if len(errors) > 5:
            response.append(f"... и ещё {len(errors)-5} ошибок")
    
    await state.clear()
    await message.answer("\n".join(response), reply_markup=get_admin_keyboard())

async def add_service_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return
    
    await message.answer(
        "Введите данные услуги в формате:\n"
        "Название услуги - Цена - Описание\n"
        "Например: Стрижка - 1000 - Мужская стрижка",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AddServiceStates.waiting_for_data)

async def process_add_service(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Действие отменено", reply_markup=get_admin_keyboard())
        return
    
    try:
        name, price, description = map(str.strip, message.text.split('-', 2))
        async with SessionLocal() as session:
            session.add(Service(name=name, price=price, description=description))
            await session.commit()
        await message.answer(
            f"Услуга '{name}' успешно добавлена!",
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        await message.answer(
            f"Ошибка при добавлении услуги: {e}\n"
            "Пожалуйста, проверьте формат ввода и попробуйте еще раз",
            reply_markup=get_cancel_keyboard()
        )
    finally:
        await state.clear()

async def edit_service_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return
    
    keyboard = await get_services_keyboard()
    if not keyboard:
        await message.answer("Нет доступных услуг для редактирования")
        return
    
    await message.answer(
        "Выберите услугу для редактирования:",
        reply_markup=keyboard
    )
    await state.set_state(EditServiceStates.waiting_for_service)

async def select_service_to_edit(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.clear()
        await message.answer("Действие отменено", reply_markup=get_admin_keyboard())
        return
    
    service_name = message.text.split(' - ')[0]
    await state.update_data(service_name=service_name)
    await message.answer(
        f"Введите новые данные для услуги '{service_name}' в формате:\n"
        "Новое название - Новая цена - Новое описание\n",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(EditServiceStates.waiting_for_new_data)

async def process_edit_service(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Действие отменено", reply_markup=get_admin_keyboard())
        return
    
    data = await state.get_data()
    service_name = data['service_name']
    
    async with SessionLocal() as session:
        service = await session.execute(
            select(Service).where(Service.name == service_name)
        )
        service = service.scalars().first()
        
        if not service:
            await message.answer("Услуга не найдена")
            await state.clear()
            return
        
        try:
            new_name, new_price, new_description = map(str.strip, message.text.split('-', 2))
            
            if new_name != '-':
                service.name = new_name
            if new_price != '-':
                service.price = new_price
            if new_description != '-':
                service.description = new_description
            
            await session.commit()
            await message.answer(
                f"Услуга успешно обновлена!\n"
                f"Название: {service.name}\n"
                f"Цена: {service.price}\n"
                f"Описание: {service.description}",
                reply_markup=get_admin_keyboard()
            )
        except Exception as e:
            await message.answer(
                f"Ошибка при обновлении услуги: {e}\n"
                "Пожалуйста, проверьте формат ввода и попробуйте еще раз",
                reply_markup=get_cancel_keyboard()
            )
        finally:
            await state.clear()

async def delete_service_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return
    
    keyboard = await get_services_keyboard()
    if not keyboard:
        await message.answer("Нет доступных услуг для удаления")
        return
    
    await message.answer(
        "Выберите услугу для удаления:",
        reply_markup=keyboard
    )
    await state.set_state(DeleteServiceStates.waiting_for_service)

async def delete_service_confirm(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.clear()
        await message.answer("Действие отменено", reply_markup=get_admin_keyboard())
        return
    
    service_name = message.text.split(' - ')[0]
    
    async with SessionLocal() as session:
        service = await session.execute(
            select(Service).where(Service.name == service_name)
        )
        service = service.scalars().first()
        
        if not service:
            await message.answer("Услуга не найдена")
            await state.clear()
            return
        
        bookings = await session.execute(
            select(Booking).where(Booking.service_id == service.id)
        )
        
        if bookings.scalars().first():
            await message.answer(
                "Нельзя удалить услугу, на которую есть записи. "
                "Сначала удалите или перенесите все записи на эту услугу.",
                reply_markup=get_admin_keyboard()
            )
            await state.clear()
            return
        
        await session.delete(service)
        await session.commit()
    
    await state.clear()
    await message.answer(
        f"Услуга '{service_name}' успешно удалена!",
        reply_markup=get_admin_keyboard()
    )

async def start_broadcast(message: types.Message, state: FSMContext):
    """Начало процесса рассылки"""
    if message.from_user.id != ADMIN_ID:  # ADMIN_ID должен быть определен в конфигурации
        return
    
    await message.answer(
        "✉️ Введите сообщение для рассылки:",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(AdminStates.waiting_for_broadcast_message)

async def process_broadcast_message(message: types.Message, state: FSMContext, bot: Bot):
    """Обработка сообщения для рассылки"""
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Рассылка отменена", reply_markup=get_admin_keyboard())
        return
    
    data = await state.get_data()
    start_time = data.get('start_time', datetime.now())
    
    try:
        async with SessionLocal() as session:
            users = await session.execute(
                select(User.telegram_id).where(User.telegram_id != str(ADMIN_ID))
            )
            users = users.scalars().all()
        
        success = 0
        failed = []
        
        for user_id in users:
            try:
                await bot.send_message(
                    chat_id=int(user_id),
                    text=message.html_text if hasattr(message, 'html_text') else message.text,
                    parse_mode="HTML"
                )
                success += 1
                await asyncio.sleep(0.1)  # Защита от лимитов Telegram
            except Exception as e:
                failed.append(str(user_id))
                logger.warning(f"Не удалось отправить сообщение {user_id}: {e}")
        
        time_spent = (datetime.now() - start_time).total_seconds()
        
        report = (
            f"📊 Результаты рассылки\n\n"
            f"• Получателей: {len(users)}\n"
            f"• Успешно: {success}\n"
            f"• Ошибки: {len(failed)}\n"
            f"• Время: {time_spent:.2f} сек.\n\n"
            f"Первые 10 ID с ошибками:\n{', '.join(failed[:10])}{'...' if len(failed) > 10 else ''}"
        )
        
        await message.answer(report, reply_markup=get_admin_keyboard())
    
    except Exception as e:
        logger.error(f"Ошибка рассылки: {e}", exc_info=True)
        await message.answer(f"❌ Критическая ошибка: {str(e)}", reply_markup=get_admin_keyboard())
    
    finally:
        await state.clear()
        
async def broadcast_handler(message: types.Message, state: FSMContext):
    """Обработчик начала рассылки"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("🚫 Эта функция доступна только администратору", reply_markup=get_admin_keyboard())
        return
    
    await state.update_data(start_time=datetime.now())
    await message.answer(
        "✉️ Введите сообщение для рассылки:\n\n"
        "Для отмены нажмите кнопку ниже",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_broadcast_message)

async def broadcast_send(message: types.Message, state: FSMContext):
    bot = message.bot
    
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Рассылка отменена", reply_markup=get_admin_keyboard())
        return
    
    async with SessionLocal() as session:
        users = await session.execute(select(User))
        users = users.scalars().all()
        
        success = 0
        failed = 0
        
        for user in users:
            try:
                await bot.send_message(
                    chat_id=user.telegram_id,
                    text=f"📢 Сообщение от администратора:\n\n{message.text}"
                )
                success += 1
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения пользователю {user.id}: {e}")
                failed += 1
    
    await state.clear()
    await message.answer(
        f"Рассылка завершена:\nУспешно: {success}\nНе удалось: {failed}",
        reply_markup=get_admin_keyboard()
    )

async def view_schedule_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return
    
    async with SessionLocal() as session:
        dates = await session.execute(
            select(Schedule.date)
            .where(Schedule.date >= datetime.now())
            .order_by(Schedule.date)
        )
        
        dates = dates.scalars().all()
        
        if not dates:
            await message.answer("Расписание не создано или все слоты уже прошли")
            return
        
        schedule_by_date = {}
        for slot in dates:
            date_str = slot.strftime('%d.%m.%Y')
            time_str = slot.strftime('%H:%M')
            if date_str not in schedule_by_date:
                schedule_by_date[date_str] = []
            schedule_by_date[date_str].append(time_str)
        
        response = "📅 Текущее расписание:\n\n"
        for date, times in schedule_by_date.items():
            response += f"📅 <b>{date}</b>:\n"
            for time in sorted(times):
                response += f"  - {time}\n"
            response += "\n"
        
        booked_slots = await session.execute(
            select(Booking.date)
            .where(Booking.date >= datetime.now(), Booking.confirmed == True)
        )
        booked_slots = {slot.strftime('%d.%m.%Y %H:%M') for slot in booked_slots.scalars()}
        
        total_slots = len(dates)
        booked_count = len(booked_slots)
        free_slots = total_slots - booked_count
        
        response += (
            f"\nℹ️ <b>Статистика:</b>\n"
            f"Всего слотов: {total_slots}\n"
            f"Забронировано: {booked_count}\n"
            f"Свободно: {free_slots}"
        )
        
        await message.answer(response, parse_mode='HTML')

async def client_functions_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return
    
    await message.answer(
        "Переключение в клиентский режим",
        reply_markup=get_client_keyboard()
    )

async def view_feedbacks_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Эта функция доступна только администратору")
        return

    async with SessionLocal() as session:
        try:
            feedbacks = await session.execute(
                select(Feedback, User)
                .join(User)
                .order_by(Feedback.created_at.desc())
                .limit(10)
            )
            
            if not feedbacks:
                await message.answer("Пока нет отзывов")
                return
                
            response = "📝 Последние отзывы:\n\n"
            for feedback, user in feedbacks:
                response += (
                    f"👤 {user.first_name} {user.last_name}\n"
                    f"⭐ Оценка: {feedback.rating}/5\n"
                    f"📄 Текст: {feedback.text}\n"
                    f"📅 Дата: {feedback.created_at.strftime('%d.%m.%Y %H:%M')}\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                )
            
            await message.answer(response, parse_mode="HTML")
            
        except Exception as e:
            logging.error(f"Error fetching feedbacks: {e}")
            await message.answer("Ошибка при получении отзывов")