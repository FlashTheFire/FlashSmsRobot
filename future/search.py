import telebot
import random
import time
from concurrent.futures import ThreadPoolExecutor

# Replace with your actual bot token.
API_TOKEN = '6609334746:AAEvl5LqcIvRYeWF85V2Vecp6qUzz35qiIM'
bot = telebot.TeleBot(API_TOKEN)

def generate_hard_question():
    """
    Simulated AI agent: generates one unique, hard quiz question.
    Randomly selects a question type from arithmetic, sequence, or algebra.
    """
    q_type = random.choice(["arithmetic", "sequence", "algebra"])
    
    if q_type == "arithmetic":
        # Generate an arithmetic expression question.
        a = random.randint(10, 50)
        b = random.randint(2, 10)
        c = random.randint(5, 30)
        d = random.randint(1, 10)
        answer = a * b + c - d
        question_text = f"What is the result of {a} * {b} + {c} - {d}?"
        # Generate 3 unique distractors.
        distractors = set()
        while len(distractors) < 3:
            delta = random.randint(-10, 10)
            option = answer + delta
            if option != answer:
                distractors.add(option)
        options = list(distractors) + [answer]
        random.shuffle(options)
        correct_index = options.index(answer)
        explanation = f"The answer is calculated as {a} * {b} + {c} - {d} = {answer}."
    
    elif q_type == "sequence":
        # Generate an arithmetic sequence puzzle.
        start = random.randint(1, 20)
        step = random.randint(2, 10)
        sequence = [start + i * step for i in range(4)]
        next_number = start + 4 * step
        question_text = f"Find the next number in the sequence: {', '.join(map(str, sequence))}."
        distractors = set()
        while len(distractors) < 3:
            delta = random.randint(-5, 5)
            option = next_number + delta
            if option != next_number:
                distractors.add(option)
        options = list(distractors) + [next_number]
        random.shuffle(options)
        correct_index = options.index(next_number)
        explanation = f"The sequence increases by {step} each time. The next number is {next_number}."
    
    else:  # algebra
        # Generate a simple algebra question: Solve for x in a*x + b = c.
        a = random.randint(1, 10)
        x = random.randint(1, 20)
        b_val = random.randint(1, 20)
        c_val = a * x + b_val
        question_text = f"Solve for x: {a}x + {b_val} = {c_val}."
        answer = x
        distractors = set()
        while len(distractors) < 3:
            delta = random.randint(-5, 5)
            option = answer + delta
            if option != answer and option > 0:
                distractors.add(option)
        options = list(distractors) + [answer]
        random.shuffle(options)
        correct_index = options.index(answer)
        explanation = f"Solving the equation: {a}x + {b_val} = {c_val}, we get x = {answer}."
    
    return {
        "question": question_text,
        "options": [str(opt) for opt in options],
        "correct_option": correct_index,
        "explanation": explanation
    }

class QuizSession:
    def __init__(self, user_id):
        self.user_id = user_id
        self.questions = []
        self.current_index = 0
        self.correct_count = 0
        self.start_time = time.time()
        self.last_question_time = time.time()
        self.poll_id_mapping = {}
        # Generate 10 unique hard questions concurrently using AI workers.
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(generate_hard_question) for _ in range(10)]
            for future in futures:
                self.questions.append(future.result())
    
    def get_current_question(self):
        if self.current_index < len(self.questions):
            return self.questions[self.current_index]
        return None

    def record_poll(self, poll_id):
        self.poll_id_mapping[poll_id] = self.current_index

# In-memory storage for active quiz sessions.
user_sessions = {}

@bot.message_handler(commands=['quiz'])
def start_quiz(message):
    user_id = message.from_user.id
    # Prevent multiple concurrent sessions for a single user.
    if user_id in user_sessions:
        bot.reply_to(message, "You are already in an active quiz session!")
        return

    # Create a new quiz session with 10 AI-generated questions.
    session = QuizSession(user_id)
    user_sessions[user_id] = session
    send_next_question(message.chat.id, session)

def send_next_question(chat_id, session: QuizSession):
    question_obj = session.get_current_question()
    if question_obj is None:
        finish_quiz(chat_id, session)
        return

    # Security check: prevent too-fast answering (less than 1 second per question).
    current_time = time.time()
    '''if current_time - session.last_question_time < 1:
        bot.send_message(chat_id, "Suspicious behavior detected: answering too quickly. Session terminated.")
        if session.user_id in user_sessions:
            del user_sessions[session.user_id]
        return'''

    session.last_question_time = current_time

    # Send the quiz poll (Telegram quiz type automatically verifies the answer).
    sent_poll = bot.send_poll(
        chat_id=chat_id,
        question=question_obj["question"],
        options=question_obj["options"],
        type='quiz',                # Specifies this is a quiz poll.
        correct_option_id=question_obj["correct_option"],
        explanation=question_obj["explanation"],
        open_period=30,             # Poll is open for 30 seconds.
        is_anonymous=False         # Non-anonymous responses tie answers securely to the user.
    )
    session.record_poll(sent_poll.poll.id)

@bot.poll_answer_handler()
def handle_poll_answer(poll_answer):
    user_id = poll_answer.user.id
    if user_id not in user_sessions:
        return  # No active session; ignore answer.

    session = user_sessions[user_id]
    poll_id = poll_answer.poll_id
    # Validate that this poll belongs to the current session.
    if poll_id not in session.poll_id_mapping:
        return

    question_index = session.poll_id_mapping[poll_id]
    # Ensure this answer is for the expected (current) question.
    if question_index != session.current_index:
        return

    question_obj = session.questions[question_index]
    # Check the answer.
    if poll_answer.option_ids and poll_answer.option_ids[0] == question_obj["correct_option"]:
        session.correct_count += 1

    session.current_index += 1
    chat_id = poll_answer.user.id

    if session.current_index < len(session.questions):
        send_next_question(chat_id, session)
    else:
        finish_quiz(chat_id, session)

def finish_quiz(chat_id, session: QuizSession):
    if session.correct_count == len(session.questions):
        bot.send_message(chat_id, "Congratulations! You answered all questions correctly and earned a bonus!")
    else:
        bot.send_message(chat_id, f"Quiz finished. You answered {session.correct_count}/{len(session.questions)} correctly. No bonus awarded.")
    
    if session.user_id in user_sessions:
        del user_sessions[session.user_id]

if __name__ == '__main__':
    print("Bot is running securely...")
    bot.polling(none_stop=True)
