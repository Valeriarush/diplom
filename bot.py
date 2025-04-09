import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

from config import TOKEN
from database import init_db
from handlers.admin import (
    process_broadcast_message,
    view_bookings_handler,
    create_schedule_handler,
    create_schedule_process,
    add_service_handler,
    process_add_service,
    edit_service_handler,
    select_service_to_edit,
    process_edit_service,
    delete_service_handler,
    delete_service_confirm,
    broadcast_handler,
    broadcast_send,
    view_bookings_select_day,
    view_bookings_select_month,
    view_feedbacks_handler,
    view_schedule_handler,
    client_functions_handler
)
from handlers.user import (
    cancel_select_booking,
    feedback_handler,
    process_feedback_rating,
    process_feedback_text,
    process_reschedule_confirmation,
    send_booking_reminders,
    start_handler,
    process_first_name,
    process_last_name,
    process_phone,
    start_booking,
    select_service,
    select_month,
    select_day,
    select_time,
    my_bookings_handler,
    reschedule_handler,
    reschedule_select_booking,
    reschedule_new_month,
    reschedule_new_day,
    reschedule_new_time,
    cancel_handler,
    cancel_confirm,
    process_booking_confirmation,
    process_booking_actions,
    process_cancel_confirmation,
    process_rebooking
)
from states import AddServiceStates, BookingStates, AdminStates, CancelStates, CreateScheduleStates, DeleteServiceStates, EditServiceStates, FeedbackStates, RegistrationStates, RescheduleStates, ViewBookingsStates

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def main():
    try:
        await init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return

    storage = MemoryStorage()
    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=storage)
    
    # Регистрация обработчиков
    dp.message.register(start_handler, Command("start"))
    
    # Административные обработчики
    dp.message.register(view_bookings_handler, lambda m: m.text == "👁️ Просмотр записей")
    dp.message.register(create_schedule_handler, lambda m: m.text == "📅 Создать расписание")
    dp.message.register(add_service_handler, lambda m: m.text == "📝 Добавить услугу")
    dp.message.register(edit_service_handler, lambda m: m.text == "✏️ Редактировать услугу")
    dp.message.register(delete_service_handler, lambda m: m.text == "🗑️ Удалить услугу")
    dp.message.register(broadcast_handler, lambda m: m.text == "📢 Рассылка")
    dp.message.register(view_schedule_handler, lambda m: m.text == "📋 Посмотреть расписание")
    dp.message.register(client_functions_handler, lambda m: m.text == "📋 Клиентские функции")
    dp.message.register(view_feedbacks_handler, lambda m: m.text == "📝 Посмотреть отзывы")
    
    # Состояния администратора
    dp.message.register(create_schedule_process, CreateScheduleStates.waiting_for_dates)
    dp.message.register(process_add_service, AddServiceStates.waiting_for_data)
    dp.message.register(select_service_to_edit, EditServiceStates.waiting_for_service)
    dp.message.register(process_edit_service, EditServiceStates.waiting_for_new_data)
    dp.message.register(delete_service_confirm, DeleteServiceStates.waiting_for_service)
    dp.message.register(view_bookings_select_month, ViewBookingsStates.waiting_for_month)
    dp.message.register(view_bookings_select_day, ViewBookingsStates.waiting_for_day)
    dp.message.register(process_broadcast_message, AdminStates.waiting_for_broadcast_message)
    
    # Клиентские обработчики
    dp.message.register(start_booking, lambda m: m.text == "💈 Записаться на услугу")
    dp.message.register(my_bookings_handler, lambda m: m.text == "📋 Мои записи")
    dp.message.register(reschedule_handler, lambda m: m.text == "🔄 Перенести запись")
    dp.message.register(cancel_handler, lambda m: m.text == "❌ Отменить запись")
    dp.message.register(feedback_handler, lambda m: m.text == "📝 Оставить отзыв")
    dp.message.register(cancel_select_booking, CancelStates.waiting_for_booking)
    dp.message.register(cancel_confirm, CancelStates.waiting_for_confirmation)
    dp.message.register(process_feedback_text, FeedbackStates.waiting_for_feedback_text)
    dp.message.register(process_feedback_rating, FeedbackStates.waiting_for_feedback_rating)
    dp.callback_query.register(process_cancel_confirmation, CancelStates.waiting_for_confirmation)
    
    # Состояния регистрации
    dp.message.register(process_first_name, RegistrationStates.waiting_for_first_name)
    dp.message.register(process_last_name, RegistrationStates.waiting_for_last_name)
    dp.message.register(process_phone, RegistrationStates.waiting_for_phone)
    
    # Состояния записи на услугу
    dp.message.register(select_service, BookingStates.waiting_for_service)
    dp.message.register(select_month, BookingStates.waiting_for_month)
    dp.message.register(select_day, BookingStates.waiting_for_day)
    dp.message.register(select_time, BookingStates.waiting_for_time)
    
    # Состояния переноса записи
    dp.message.register(reschedule_select_booking, RescheduleStates.waiting_for_booking)
    dp.message.register(reschedule_new_month, RescheduleStates.waiting_for_new_month)
    dp.message.register(reschedule_new_day, RescheduleStates.waiting_for_new_day)
    dp.message.register(reschedule_new_time, RescheduleStates.waiting_for_new_time)
    
    # Callback-обработчики
    dp.callback_query.register(reschedule_select_booking, lambda c: c.data.startswith('select_reschedule_')) 
    dp.callback_query.register(process_booking_confirmation, lambda c: c.data.startswith(('confirm_', 'cancel_')))
    dp.callback_query.register(process_booking_actions, lambda c: c.data.startswith(('reschedule_', 'cancel_')))
    dp.callback_query.register(process_cancel_confirmation, lambda c: c.data.startswith('confirm_cancel_') or c.data == 'keep_booking')
    dp.callback_query.register(process_rebooking, lambda c: c.data.startswith('rebook_'))
    
    # Планировщик для напоминаний
    scheduler = AsyncIOScheduler()
    # Передаем экземпляр бота в функцию напоминаний
    scheduler.add_job(
        send_booking_reminders,
        'interval',
        minutes=30,
        args=[bot],  # Передаем бота как аргумент
        next_run_time=datetime.now()
    )
    scheduler.start()

    try:
        logger.info("Бот запущен")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка в работе бота: {e}")
    finally:
        scheduler.shutdown()
        await bot.session.close()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())