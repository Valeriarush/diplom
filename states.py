from aiogram.fsm.state import State, StatesGroup

class RegistrationStates(StatesGroup):
    waiting_for_first_name = State()
    waiting_for_last_name = State()
    waiting_for_phone = State()

class BookingStates(StatesGroup):
    waiting_for_service = State()
    waiting_for_month = State()
    waiting_for_day = State()
    waiting_for_time = State()

class RescheduleStates(StatesGroup):
    waiting_for_booking = State()
    waiting_for_new_month = State()
    waiting_for_new_day = State()
    waiting_for_new_time = State()

class CancelStates(StatesGroup):
    waiting_for_booking = State()
    waiting_for_confirmation = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()

class AddServiceStates(StatesGroup):
    waiting_for_data = State()
    waiting_for_broadcast_message = State()

class EditServiceStates(StatesGroup):
    waiting_for_service = State()
    waiting_for_new_data = State()

class DeleteServiceStates(StatesGroup):
    waiting_for_service = State()

class CreateScheduleStates(StatesGroup):
    waiting_for_dates = State()

class FeedbackStates(StatesGroup):
    waiting_for_feedback_text = State()
    waiting_for_feedback_rating = State()

class AdminAuthStates(StatesGroup):
    waiting_for_admin_password = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_message = State()

class UserProfileStates(StatesGroup):
    waiting_for_new_first_name = State()
    waiting_for_new_last_name = State()
    waiting_for_new_phone = State()
class ViewBookingsStates(StatesGroup):
    waiting_for_month = State()
    waiting_for_day = State()