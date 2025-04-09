# handlers/user.py
from email import message
import re
import logging
from datetime import datetime, timedelta
import stat
from aiogram import Bot, types
from aiogram.fsm.context import FSMContext
from sqlalchemy import select, func, and_, exists
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from states import FeedbackStates
from database import SessionLocal
from models import Feedback, User, Service, Booking, Schedule
from states import (
    RegistrationStates, BookingStates,
    RescheduleStates, CancelStates
)
from keyboards import (
    get_client_keyboard, get_admin_keyboard,
    get_cancel_keyboard, get_months_keyboard, get_services_keyboard,
    get_days_keyboard_for_month, get_times_keyboard,
    get_confirm_keyboard, get_user_bookings_keyboard,
)
from config import ADMIN_ID

logger = logging.getLogger(__name__)

def is_valid_name(name: str) -> bool:
    return 2 <= len(name) <= 50 and name.isalpha()

def is_valid_phone(phone: str) -> bool:
    return re.match(r'^\+?[1-9]\d{10,14}$', phone) is not None

def is_valid_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except ValueError:
        return False

def is_valid_time(time_str: str) -> bool:
    try:
        datetime.strptime(time_str, '%H:%M')
        return True
    except ValueError:
        return False

def can_modify_booking(booking_date: datetime) -> bool:
    return (booking_date - datetime.now()) > timedelta(hours=24)

async def start_handler(message: types.Message, state: FSMContext):
    if message.from_user.id == ADMIN_ID:
        await message.answer("👑 Добро пожаловать, администратор!", reply_markup=get_admin_keyboard())
        return
    
    async with SessionLocal() as session:
        user = await session.execute(
            select(User).where(User.telegram_id == str(message.from_user.id))
        )
        user = user.scalars().first()
        
        if user:
            await message.answer("👋 С возвращением!", reply_markup=get_client_keyboard())
            return
    
    await message.answer(
        "Добро пожаловать! Для начала давайте зарегистрируемся.\nВведите ваше имя:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(RegistrationStates.waiting_for_first_name)

async def process_first_name(message: types.Message, state: FSMContext):
    if not is_valid_name(message.text):
        await message.answer("Имя должно содержать только буквы (2-50 символов). Попробуйте еще раз.")
        return
        
    await state.update_data(first_name=message.text)
    await message.answer("Теперь введите вашу фамилию:")
    await state.set_state(RegistrationStates.waiting_for_last_name)

async def process_last_name(message: types.Message, state: FSMContext):
    if not is_valid_name(message.text):
        await message.answer("Фамилия должна содержать только буквы (2-50 символов). Попробуйте еще раз.")
        return
        
    await state.update_data(last_name=message.text)
    await message.answer("Введите ваш номер телефона в формате +79998887766:")
    await state.set_state(RegistrationStates.waiting_for_phone)

async def process_phone(message: types.Message, state: FSMContext):
    if not is_valid_phone(message.text):
        await message.answer("Некорректный формат телефона. Попробуйте еще раз.")
        return
    
    data = await state.get_data()
    async with SessionLocal() as session:
        user = User(
            telegram_id=str(message.from_user.id),
            first_name=data['first_name'],
            last_name=data['last_name'],
            phone=message.text
        )
        session.add(user)
        await session.commit()
    
    await state.clear()
    await message.answer(
        "Регистрация завершена! Теперь вы можете записаться на услугу.",
        reply_markup=get_client_keyboard()
    )

async def start_booking(message: types.Message, state: FSMContext):
    await message.answer(
        "Выберите услугу:",
        reply_markup=await get_services_keyboard()
    )
    await state.set_state(BookingStates.waiting_for_service)

async def select_service(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.clear()
        await message.answer("Главное меню", reply_markup=get_client_keyboard())
        return
    
    try:
        service_name = message.text.split(' - ')[0]
    except IndexError:
        await message.answer("Пожалуйста, выберите услугу из списка")
        return
    
    async with SessionLocal() as session:
        service = await session.execute(select(Service).where(Service.name == service_name))
        service = service.scalars().first()
        
        if not service:
            await message.answer("Услуга не найдена, попробуйте ещё раз")
            return
        
        await state.update_data(service_id=service.id, service_name=service.name)
        await message.answer(
            "Выберите месяц:",
            reply_markup=await get_months_keyboard()
        )
        await state.set_state(BookingStates.waiting_for_month)

async def select_month(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.set_state(BookingStates.waiting_for_service)
        await message.answer(
            "Выберите услугу:",
            reply_markup=await get_services_keyboard()
        )
        return
    
    await state.update_data(month=message.text)
    await message.answer(
        "Выберите день:",
        reply_markup=await get_days_keyboard_for_month(message.text)
    )
    await state.set_state(BookingStates.waiting_for_day)

async def select_day(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        data = await state.get_data()
        await message.answer(
            "Выберите месяц:",
            reply_markup=await get_months_keyboard()
        )
        await state.set_state(BookingStates.waiting_for_month)
        return
    
    if not is_valid_date(message.text):
        await message.answer("Некорректный формат даты. Выберите день из списка.")
        return
    
    await state.update_data(day=message.text)
    await message.answer(
        "Выберите время:",
        reply_markup=await get_times_keyboard(message.text)
    )
    await state.set_state(BookingStates.waiting_for_time)

async def select_time(message: types.Message, state: FSMContext):
    try:
        if message.text == "🔙 Назад":
            data = await state.get_data()
            await message.answer(
                "Выберите день:",
                reply_markup=await get_days_keyboard_for_month(data['month'])
            )
            await state.set_state(BookingStates.waiting_for_day)
            return
        
        if not is_valid_time(message.text):
            await message.answer("Некорректный формат времени. Выберите время из списка.")
            return
        
        data = await state.get_data()
        selected_datetime = datetime.strptime(
            f"{data['day']} {message.text}", '%d.%m.%Y %H:%M'
        )
        
        async with SessionLocal() as session:
            async with session.begin():  # Явное управление транзакцией
                schedule_slot = await session.execute(
                    select(Schedule)
                    .where(Schedule.date == selected_datetime)
                    .with_for_update()  # Блокируем запись для обновления
                )
                schedule_slot = schedule_slot.scalars().first()
                
                if not schedule_slot:
                    await message.answer("Это время больше не доступно")
                    return
                
                existing_booking = await session.execute(
                    select(Booking)
                    .where(
                        Booking.schedule_id == schedule_slot.id,
                        Booking.confirmed == True
                    )
                )
                
                if existing_booking.scalars().first():
                    await message.answer("Это время уже занято, выберите другое")
                    return
                
                user = await session.execute(
                    select(User).where(User.telegram_id == str(message.from_user.id))
                )
                user = user.scalars().first()
                
                booking = Booking(
                    date=selected_datetime,
                    user_id=user.id,
                    service_id=data['service_id'],
                    confirmed=True,
                    schedule_id=schedule_slot.id
                )
                session.add(booking)
                
                await message.answer(
                    f"✅ Вы успешно записаны на {data['service_name']}!\n"
                    f"📅 Дата и время: {selected_datetime.strftime('%d.%m.%Y %H:%M')}",
                    reply_markup=get_client_keyboard()
                )
            
            await state.clear()
            
    except Exception as e:
        logger.error(f"Ошибка при создании записи: {str(e)}", exc_info=True)
        await message.answer("Произошла ошибка при обработке вашей записи. Пожалуйста, попробуйте позже.")

async def my_bookings_handler(message: types.Message):
    async with SessionLocal() as session:
        user = await session.execute(
            select(User).where(User.telegram_id == str(message.from_user.id))
        )
        user = user.scalars().first()
        
        if not user:
            await message.answer("Ошибка: пользователь не найден")
            return
        
        bookings = await session.execute(
            select(Booking, Service)
            .join(Service)
            .where(
                Booking.user_id == user.id,
                Booking.date >= datetime.now()
            )
            .order_by(Booking.date)
        )
        
        bookings = bookings.all()
        
        if not bookings:
            await message.answer("У вас нет активных записей", reply_markup=get_client_keyboard())
            return
        
        response = "📋 <b>Ваши активные записи:</b>\n\n"
        for booking, service in bookings:
            status = "✅ Подтверждена" if booking.confirmed else "🕒 Ожидает подтверждения"
            response += (
                f"<b>🔹 Услуга:</b> {service.name}\n"
                f"<b>📅 Дата и время:</b> {booking.date.strftime('%d.%m.%Y %H:%M')}\n"
                f"<b>Статус:</b> {status}\n"
                f"<b>ID записи:</b> {booking.id}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
            )
        
        await message.answer(
            response,
            reply_markup=get_client_keyboard(),
            parse_mode='HTML'
        )

async def process_booking_actions(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработчик действий с записями (перенос/отмена)"""
    action, booking_id = callback_query.data.split('_')
    booking_id = int(booking_id)
    
    async with SessionLocal() as session:
        booking = await session.execute(
            select(Booking, Service)
            .join(Service)
            .where(Booking.id == booking_id)
        )
        booking, service = booking.first()
        
        if not booking or booking.user.telegram_id != str(callback_query.from_user.id):
            await callback_query.answer("Запись не найдена")
            return
        
        if action == 'reschedule':
            if not can_modify_booking(booking.date):
                await callback_query.answer("Перенос возможен не позднее чем за 24 часа до записи")
                return
            
            await state.update_data(
                booking_id=booking.id,
                service_id=booking.service_id
            )
            await callback_query.message.answer(
                "Выберите новый месяц для записи:",
                reply_markup=await get_months_keyboard()
            )
            await state.set_state(RescheduleStates.waiting_for_new_month)
            
        elif action == 'cancel':
            if not can_modify_booking(booking.date):
                await callback_query.answer("Отмена возможна не позднее чем за 24 часа до записи")
                return
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Да, отменить",
                        callback_data=f"confirm_cancel_{booking.id}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Нет, оставить",
                        callback_data="keep_booking"
                    )
                ]
            ])
            
            await callback_query.message.edit_text(
                f"Вы уверены, что хотите отменить запись на {service.name} "
                f"({booking.date.strftime('%d.%m.%Y %H:%M')})?",
                reply_markup=keyboard
            )
        
        await callback_query.answer()

async def process_cancel_confirmation(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.data == "keep_booking":
        await callback_query.message.edit_text("Отмена записи отменена")
        await callback_query.answer()
        return
    
    _, booking_id = callback_query.data.split('_')
    booking_id = int(booking_id)
    
    async with SessionLocal() as session:
        booking = await session.get(Booking, booking_id)
        if booking:
            # Вместо удаления просто помечаем как отмененную
            booking.confirmed = False
            await session.commit()

async def process_rebooking(callback_query: types.CallbackQuery, state: FSMContext):
    service_id = int(callback_query.data.split('_')[1])
    
    async with SessionLocal() as session:
        service = await session.get(Service, service_id)
        if service:
            await state.update_data(
                service_id=service.id,
                service_name=service.name
            )
            await callback_query.message.answer(
                "Выберите новый месяц для записи:",
                reply_markup=await get_months_keyboard()
            )
            await state.set_state(BookingStates.waiting_for_month)
    
    await callback_query.answer()

async def can_perform_action(user: User, action_type: str) -> bool:
    """Проверяет, может ли пользователь выполнить действие"""
    current_month = datetime.now().month
    
    # Если действие в новом месяце - сбрасываем счетчики
    if user.last_action_month != current_month:
        user.reschedules_this_month = 0
        user.cancels_this_month = 0
        user.last_action_month = current_month
        return True
    
    if action_type == 'reschedule':
        return user.reschedules_this_month < 1
    elif action_type == 'cancel':
        return user.cancels_this_month < 1
    return False

async def reschedule_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки 'Перенести запись'"""
    async with SessionLocal() as session:
        user = await session.execute(
            select(User).where(User.telegram_id == str(message.from_user.id))
        )
        user = user.scalars().first()
        
        current_month = datetime.now().month
        if user.last_action_month != current_month:
            user.reschedules_this_month = 0
            user.last_action_month = current_month
            await session.commit()
        
        # Если лимит исчерпан - сразу сообщаем
        if user.reschedules_this_month >= 1:
            await message.answer(
                "❌ Лимит переносов на этот месяц исчерпан (1/1)",
                reply_markup=get_client_keyboard()
            )
            return
        
        # Если есть попытки - сразу переходим к выбору записи
        keyboard = await get_user_bookings_keyboard(message.from_user.id)
        if not keyboard:
            await message.answer("У вас нет активных записей для переноса", 
                              reply_markup=get_client_keyboard())
            return
        
        await message.answer(
            "Вы можете перенести запись только 1 раз в месяц\n"
            "Выберите запись для переноса:",
            reply_markup=keyboard
        )
        await state.set_state(RescheduleStates.waiting_for_booking)

async def process_reschedule_confirmation(message: types.Message, state: FSMContext):
    """Обработчик подтверждения переноса"""
    if message.text == "❌ Нет":
        await state.clear()
        await message.answer(
            "Перенос записи отменен",
            reply_markup=get_client_keyboard()
        )
        return
    
    if message.text != "✅ Да":
        await message.answer("Пожалуйста, используйте кнопки")
        return
    
    async with SessionLocal() as session:
        user = await session.execute(
            select(User).where(User.telegram_id == str(message.from_user.id))
        )
        user = user.scalars().first()
        
        if user.reschedules_this_month >= 1:
            await message.answer(
                "Вы уже использовали свой лимит переносов в этом месяце",
                reply_markup=get_client_keyboard()
            )
            await state.clear()
            return
        
        keyboard = await get_user_bookings_keyboard(message.from_user.id)
        await message.answer(
            "Выберите запись для переноса:",
            reply_markup=keyboard
        )
        await state.set_state(RescheduleStates.waiting_for_booking)

async def reschedule_select_booking(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.clear()
        await message.answer("Главное меню", reply_markup=get_client_keyboard())
        return
    
    try:
        booking_id = int(message.text.split(':')[0].strip())
    except (ValueError, IndexError):
        await message.answer("Некорректный формат записи. Пожалуйста, выберите запись из списка.")
        return
    
    async with SessionLocal() as session:
        booking = await session.execute(
            select(Booking)
            .options(
                joinedload(Booking.user),
                joinedload(Booking.service)  # Убедитесь, что отношение service определено в модели
            )
            .where(Booking.id == booking_id)
        )
        booking = booking.scalars().first()
        
        if not booking or booking.user.telegram_id != str(message.from_user.id):
            await message.answer("Запись не найдена или у вас нет прав для её изменения")
            await state.clear()
            return
        
        if not can_modify_booking(booking.date):
            await message.answer("Перенос возможен не позднее чем за 24 часа до записи")
            await state.clear()
            return
        
        await state.update_data(
            booking_id=booking.id,
            service_id=booking.service.id,
            service_name=booking.service.name,
            old_date=booking.date
        )
        
        await message.answer(
            f"Перенос записи на {booking.service.name}\n"
            f"Текущая дата: {booking.date.strftime('%d.%m.%Y %H:%M')}\n\n"
            "Выберите новый месяц:",
            reply_markup=await get_months_keyboard()
        )
        await state.set_state(RescheduleStates.waiting_for_new_month)

async def reschedule_new_month(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.set_state(RescheduleStates.waiting_for_booking)
        keyboard = await get_user_bookings_keyboard(message.from_user.id)
        await message.answer("Выберите запись для переноса:", reply_markup=keyboard)
        return
    
    await state.update_data(month=message.text)
    await message.answer(
        "Выберите день для переноса:",
        reply_markup=await get_days_keyboard_for_month(message.text)
    )
    await state.set_state(RescheduleStates.waiting_for_new_day)

async def reschedule_new_day(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await state.set_state(RescheduleStates.waiting_for_new_month)
        await message.answer(
            "Выберите месяц для переноса:",
            reply_markup=await get_months_keyboard()
        )
        return
    
    if not is_valid_date(message.text):
        await message.answer("Некорректный формат даты. Выберите день из списка.")
        return
    
    await state.update_data(day=message.text)
    await message.answer(
        "Выберите время для переноса:",
        reply_markup=await get_times_keyboard(message.text)
    )
    await state.set_state(RescheduleStates.waiting_for_new_time)  

async def reschedule_new_time(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        data = await state.get_data()
        await message.answer(
            "Выберите день для переноса:",
            reply_markup=await get_days_keyboard_for_month(data['month'])
        )
        await state.set_state(RescheduleStates.waiting_for_new_day)
        return
    
    if not is_valid_time(message.text):
        await message.answer("Некорректный формат времени. Выберите время из списка.")
        return
    
    data = await state.get_data()
    
    async with SessionLocal() as session:
        try:
            # Получаем данные о переносимой записи
            old_booking = await session.get(Booking, data['booking_id'])
            if not old_booking:
                await message.answer("Ошибка: запись не найдена")
                await state.clear()
                return

            # Формируем новую дату
            day_str = data['day'] if len(data['day'].split('.')) == 3 else f"{data['day']}.{datetime.now().year}"
            new_datetime = datetime.strptime(f"{day_str} {message.text}", '%d.%m.%Y %H:%M')

            # Проверяем новый слот
            new_schedule_slot = await session.execute(
                select(Schedule)
                .where(Schedule.date == new_datetime)
            )
            new_schedule_slot = new_schedule_slot.scalars().first()
            
            if not new_schedule_slot:
                await message.answer("Это время больше не доступно")
                return

            # Проверяем, что новый слот свободен
            existing_booking = await session.execute(
                select(Booking)
                .where(
                    Booking.schedule_id == new_schedule_slot.id,
                    Booking.confirmed == True
                )
            )
            
            if existing_booking.scalars().first():
                await message.answer("Это время уже занято, выберите другое")
                return

            # Удаляем старую запись (или помечаем как неактивную)
            await session.delete(old_booking)
            
            # Создаем новую запись
            new_booking = Booking(
                date=new_datetime,
                user_id=old_booking.user_id,
                service_id=old_booking.service_id,
                confirmed=True,
                schedule_id=new_schedule_slot.id
            )
            session.add(new_booking)
            
            # Обновляем счетчик переносов у пользователя
            user = await session.get(User, old_booking.user_id)
            if user.last_action_month != datetime.now().month:
                user.reschedules_this_month = 0
                user.last_action_month = datetime.now().month
            user.reschedules_this_month += 1
            
            await session.commit()
            
            await state.clear()
            await message.answer(
                f"✅ Запись успешно перенесена на {new_datetime.strftime('%d.%m.%Y %H:%M')}!",
                reply_markup=get_client_keyboard()
            )
            
        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при переносе записи: {str(e)}")
            await message.answer(
                "Произошла ошибка при переносе записи. Пожалуйста, попробуйте позже."
            )

async def cancel_handler(message: types.Message, state: FSMContext):
    """Обработчик команды отмены записи"""
    try:
        async with SessionLocal() as session:
            # Явное начало транзакции
            async with session.begin():
                user = await session.execute(
                    select(User)
                    .where(User.telegram_id == str(message.from_user.id))
                    .execution_options(no_cache=True)
                )
                user = user.scalars().first()

                if not user:
                    await message.answer("❌ Ошибка: пользователь не найден")
                    return

                # Получаем только будущие подтвержденные записи
                now = datetime.now()
                bookings = await session.execute(
                    select(Booking, Service)
                    .join(Service)
                    .where(
                        Booking.user_id == user.id,
                        Booking.date >= now,
                        Booking.confirmed == True
                    )
                    .order_by(Booking.date)
                )
                
                bookings = bookings.all()

                if not bookings:
                    await message.answer("ℹ️ У вас нет активных записей для отмены")
                    return

                # Создаем клавиатуру
                keyboard = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(
                            text=f"{booking.id}: {service.name} на {booking.date.strftime('%d.%m.%Y %H:%M')}"
                        )]
                        for booking, service in bookings
                    ] + [[KeyboardButton(text="🔙 Назад")]],
                    resize_keyboard=True,
                    one_time_keyboard=True
                )

                await message.answer(
                    "📋 Выберите запись для отмены:",
                    reply_markup=keyboard
                )
                await state.set_state(CancelStates.waiting_for_booking)

    except Exception as e:
        logger.error(f"Ошибка в cancel_handler: {str(e)}", exc_info=True)
        await message.answer(
            "❌ Произошла ошибка при получении ваших записей. Пожалуйста, попробуйте позже.",
            reply_markup=get_client_keyboard()
        )

async def process_cancel_confirmation(message: types.Message, state: FSMContext):
    """Обработчик подтверждения отмены"""
    if message.text == "❌ Нет":
        await state.clear()
        await message.answer(
            "Отмена записи отменена",
            reply_markup=get_client_keyboard()
        )
        return
    
    if message.text != "✅ Да":
        await message.answer("Пожалуйста, используйте кнопки")
        return
    
    async with SessionLocal() as session:
        user = await session.execute(
            select(User).where(User.telegram_id == str(message.from_user.id))
        )
        user = user.scalars().first()
        
        if user.cancels_this_month >= 1:
            await message.answer(
                "Вы уже использовали свой лимит отмен в этом месяце",
                reply_markup=get_client_keyboard()
            )
            await state.clear()
            return
        
        keyboard = await get_user_bookings_keyboard(message.from_user.id)
        await message.answer(
            "Выберите запись для отмены:",
            reply_markup=keyboard
        )
        await state.set_state(CancelStates.waiting_for_booking)

async def cancel_select_booking(message: types.Message, state: FSMContext):
    """Обработчик выбора записи для отмены"""
    if message.text == "🔙 Назад":
        await state.clear()
        await message.answer("Действие отменено", reply_markup=get_client_keyboard())
        return
    
    try:
        # Улучшенный парсинг ID записи
        parts = message.text.split(':')
        if len(parts) < 2:
            raise ValueError("Неверный формат записи")
        
        booking_id = int(parts[0].strip())
        service_info = parts[1].strip()
        
        # Дополнительная проверка формата
        if not service_info or not str(booking_id).isdigit():
            raise ValueError("Неверный формат данных")

        async with SessionLocal() as session:
            async with session.begin():
                # Получаем запись с проверкой владельца
                booking = await session.execute(
                    select(Booking)
                    .options(joinedload(Booking.service))
                    .join(User)
                    .where(
                        Booking.id == booking_id,
                        User.telegram_id == str(message.from_user.id)
                    )
                )
                booking = booking.scalars().first()
                
                if not booking:
                    await message.answer("❌ Запись не найдена или у вас нет прав для её изменения")
                    await state.clear()
                    return
                
                # Проверяем, что запись еще актуальна (не в прошлом)
                if booking.date < datetime.now():
                    await message.answer("⚠️ Нельзя отменить прошедшую запись")
                    await state.clear()
                    return
                    
                # Проверяем временное ограничение (не менее чем за 24 часа)
                if (booking.date - datetime.now()) <= timedelta(hours=24):
                    await message.answer(
                        "⚠️ Отмена возможна не позднее чем за 24 часа до записи",
                        reply_markup=get_client_keyboard()
                    )
                    await state.clear()
                    return
                
                # Сохраняем данные для подтверждения
                await state.update_data(booking_id=booking.id)
                
                # Формируем информативное сообщение
                service_name = booking.service.name if booking.service else "неизвестная услуга"
                booking_time = booking.date.strftime('%d.%m.%Y %H:%M')
                
                # Создаем клавиатуру подтверждения
                keyboard = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="✅ Да, отменить запись")],
                        [KeyboardButton(text="❌ Нет, оставить запись")]
                    ],
                    resize_keyboard=True,
                    one_time_keyboard=True
                )
                
                await message.answer(
                    f"❓ Вы уверены, что хотите отменить запись?\n\n"
                    f"🔹 Услуга: {service_name}\n"
                    f"📅 Дата и время: {booking_time}\n\n"
                    f"После отмены запись будет удалена безвозвратно.",
                    reply_markup=keyboard
                )
                await state.set_state(CancelStates.waiting_for_confirmation)
                
    except ValueError as e:
        await message.answer(
            "⚠️ Пожалуйста, выберите запись из предложенного списка",
            reply_markup=get_client_keyboard()
        )
        logger.warning(f"Некорректный ввод при отмене записи: {str(e)}")
        
    except Exception as e:
        logger.error(f"Ошибка при отмене записи: {str(e)}", exc_info=True)
        await message.answer(
            "⚠️ Произошла ошибка при обработке вашего выбора. Пожалуйста, попробуйте еще раз или обратитесь в поддержку.",
            reply_markup=get_client_keyboard()
        )
        await state.clear()

async def cancel_confirm(message: types.Message, state: FSMContext):
    """Обработчик подтверждения отмены записи"""
    if message.text == "❌ Нет, оставить запись":
        await state.clear()
        await message.answer(
            "✅ Отмена записи отменена",
            reply_markup=get_client_keyboard()
        )
        return

    if message.text != "✅ Да, отменить запись":
        await message.answer("⚠️ Пожалуйста, используйте кнопки для подтверждения")
        return

    try:
        data = await state.get_data()
        booking_id = data.get('booking_id')
        
        if not booking_id:
            await message.answer("❌ Ошибка: не найден ID записи")
            await state.clear()
            return

        async with SessionLocal() as session:
            async with session.begin():
                # Получаем запись с блокировкой для обновления
                booking = await session.execute(
                    select(Booking)
                    .join(User)
                    .where(
                        Booking.id == booking_id,
                        User.telegram_id == str(message.from_user.id)
                    )
                    .with_for_update()
                )
                booking = booking.scalars().first()

                if not booking:
                    await message.answer("❌ Запись не найдена")
                    await state.clear()
                    return

                # Дополнительная проверка временного ограничения
                if (booking.date - datetime.now()) <= timedelta(hours=24):
                    await message.answer(
                        "⚠️ Срок отмены истёк (менее 24 часов до записи)",
                        reply_markup=get_client_keyboard()
                    )
                    await state.clear()
                    return

                # Получаем пользователя для обновления счетчика
                user = await session.get(User, booking.user_id)
                current_month = datetime.now().month
                
                # Сбрасываем счетчики если месяц сменился
                if user.last_action_month != current_month:
                    user.reschedules_this_month = 0
                    user.cancels_this_month = 0
                    user.last_action_month = current_month
                
                # Проверяем лимит отмен
                if user.cancels_this_month >= 1:
                    await message.answer(
                        "⚠️ Лимит отмен в этом месяце исчерпан (1/1)",
                        reply_markup=get_client_keyboard()
                    )
                    await state.clear()
                    return

                # Удаляем запись
                await session.delete(booking)
                user.cancels_this_month += 1
                
                await message.answer(
                    "✅ Запись успешно отменена",
                    reply_markup=get_client_keyboard()
                )
                
    except Exception as e:
        logger.error(f"Ошибка в cancel_confirm: {str(e)}", exc_info=True)
        await message.answer(
            "❌ Произошла ошибка при отмене записи",
            reply_markup=get_client_keyboard()
        )
    finally:
        await state.clear()

async def process_booking_confirmation(callback_query: types.CallbackQuery):
    """Обработчик подтверждения/отмены записи через inline-кнопку"""
    try:
        # Разбираем callback data
        try:
            action, booking_id = callback_query.data.split('_')
            booking_id = int(booking_id)
        except ValueError:
            await callback_query.answer("Некорректный запрос")
            return

        async with SessionLocal() as session:
            async with session.begin():  # Явная транзакция
                # Получаем запись с проверкой владельца и блокировкой
                booking = await session.execute(
                    select(Booking)
                    .options(joinedload(Booking.service))  # Жадная загрузка service
                    .where(Booking.id == booking_id)
                    .join(User)
                    .where(User.telegram_id == str(callback_query.from_user.id))
                    .with_for_update()  # Блокировка для конкурентного доступа
                )
                booking = booking.scalars().first()

                if not booking:
                    await callback_query.answer("Запись не найдена или нет прав")
                    return

                if action == 'confirm':
                    # Проверяем, что время ещё доступно
                    schedule_available = await session.execute(
                        select(exists().where(
                            Schedule.id == booking.schedule_id,
                            ~exists().where(
                                Booking.schedule_id == booking.schedule_id,
                                Booking.confirmed == True,
                                Booking.id != booking.id
                            )
                        ))
                    )
                    if not schedule_available.scalar():
                        await callback_query.message.edit_text(
                            "⚠️ Это время стало недоступно. Пожалуйста, выберите другое."
                        )
                        await callback_query.answer()
                        return

                    booking.confirmed = True
                    response_text = (
                        f"✅ Запись на {booking.service.name} "
                        f"({booking.date.strftime('%d.%m.%Y %H:%M')}) подтверждена!"
                    )
                else:
                    # Проверяем временное ограничение для отмены
                    if (booking.date - datetime.now()) <= timedelta(hours=24):
                        await callback_query.message.edit_text(
                            "❌ Отмена невозможна менее чем за 24 часа до записи"
                        )
                        await callback_query.answer()
                        return

                    await session.delete(booking)
                    response_text = (
                        f"❌ Запись на {booking.service.name} "
                        f"({booking.date.strftime('%d.%m.%Y %H:%M')}) отменена"
                    )

                await callback_query.message.edit_text(response_text)
                await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка в process_booking_confirmation: {str(e)}", exc_info=True)
        try:
            await callback_query.answer("Произошла ошибка. Попробуйте позже.")
        except:
            pass

async def feedback_handler(message: types.Message, state: FSMContext):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Администраторы не могут оставлять отзывы")
        return
        
    await message.answer(
        "Напишите ваш отзыв о нашем сервисе:",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(FeedbackStates.waiting_for_feedback_text)

async def process_feedback_text(message: types.Message, state: FSMContext):
    await state.update_data(feedback_text=message.text)
    await message.answer("Оцените сервис от 1 до 5:")
    await state.set_state(FeedbackStates.waiting_for_feedback_rating)

async def process_feedback_rating(message: types.Message, state: FSMContext):
    try:
        # Проверяем, что оценка - число от 1 до 5
        rating = int(message.text)  # Определяем переменную rating здесь
        if rating < 1 or rating > 5:
            await message.answer("Пожалуйста, введите число от 1 до 5")
            return

        data = await state.get_data()
        feedback_text = data.get('feedback_text', '')
        
        async with SessionLocal() as session:
            user = await session.execute(
                select(User).where(User.telegram_id == str(message.from_user.id))
            )
            user = user.scalars().first()
            
            if user:
                # Создаем объект Feedback с полученными данными
                new_feedback = Feedback(
                    user_id=user.id,
                    text=feedback_text,
                    rating=rating  # Используем переменную rating
                )
                session.add(new_feedback)
                await session.commit()
                await message.answer("Спасибо за ваш отзыв! 💖", reply_markup=get_client_keyboard())
            else:
                await message.answer("Ошибка: пользователь не найден")
                
    except ValueError:
        await message.answer("Пожалуйста, введите число от 1 до 5")
    except Exception as e:
        logger.error(f"Ошибка при сохранении отзыва: {e}")
        await message.answer("Произошла ошибка при сохранении отзыва")
    finally:
        await state.clear()

async def send_booking_reminders(bot: Bot):
    """Функция отправки напоминаний о записях"""
    async with SessionLocal() as session:
        async with session.begin():
            try:
                # Напоминание за 24 часа
                reminder_24h_time = datetime.now() + timedelta(hours=24)
                bookings_24h = await session.execute(
                    select(Booking, User, Service)
                    .join(User)
                    .join(Service)
                    .where(
                        func.date_trunc('hour', Booking.date) == func.date_trunc('hour', reminder_24h_time),
                        Booking.reminder_24h_sent == False,
                        Booking.confirmed == True
                    )
                    .execution_options(populate_existing=True)
                )
                bookings_24h = bookings_24h.all()

                # Напоминание за 3 часа
                reminder_3h_time = datetime.now() + timedelta(hours=3)
                bookings_3h = await session.execute(
                    select(Booking, User, Service)
                    .join(User)
                    .join(Service)
                    .where(
                        func.date_trunc('hour', Booking.date) == func.date_trunc('hour', reminder_3h_time),
                        Booking.reminder_3h_sent == False,
                        Booking.confirmed == True
                    )
                )
                bookings_3h = bookings_3h.all()

                # Отправка напоминаний за 24 часа
                for booking, user, service in bookings_24h:
                    try:
                        await bot.send_message(
                            chat_id=user.telegram_id,
                            text=f"⏰ Напоминание: у вас запись на {service.name} "
                                 f"завтра в {booking.date.strftime('%H:%M')}"
                        )
                        booking.reminder_24h_sent = True
                    except Exception as e:
                        logger.error(f"Ошибка отправки напоминания 24h пользователю {user.id}: {e}")

                # Отправка напоминаний за 3 часа
                for booking, user, service in bookings_3h:
                    try:
                        await bot.send_message(
                            chat_id=user.telegram_id,
                            text=f"⏰ Напоминание: у вас запись на {service.name} "
                                 f"через 3 часа ({booking.date.strftime('%H:%M')})"
                        )
                        booking.reminder_3h_sent = True
                    except Exception as e:
                        logger.error(f"Ошибка отправки напоминания 3h пользователю {user.id}: {e}")

                # Коммит транзакции выполнится автоматически при успешном завершении блока
                
            except Exception as e:
                logger.error(f"Ошибка в send_booking_reminders: {e}", exc_info=True)
                # Откат транзакции выполнится автоматически из-за исключения
                raise  # Планировщик сам обработает это исключение