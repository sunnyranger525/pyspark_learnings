# quiz_project.py

# Step 1: Store questions in a list of dictionaries
questions = [
    {
        "question": "What is the capital of India?",
        "options": ["A. Mumbai", "B. New Delhi", "C. Kolkata", "D. Chennai"],
        "answer": "B"
    },
    {
        "question": "Which keyword is used to define a function in Python?",
        "options": ["A. function", "B. def", "C. fun", "D. define"],
        "answer": "B"
    },
    {
        "question": "Which company developed the Java language?",
        "options": ["A. Microsoft", "B. Apple", "C. Sun Microsystems", "D. Google"],
        "answer": "C"
    },
    {
        "question": "What is 10 + 5?",
        "options": ["A. 10", "B. 15", "C. 20", "D. 25"],
        "answer": "B"
    }
]

# Step 2: Function to run quiz
def run_quiz():
    print("\n===== Welcome to the Quiz Game =====\n")
    score = 0

    # Loop through each question
    for i, q in enumerate(questions, start=1):
        print(f"\nQ{i}. {q['question']}")
        for opt in q["options"]:
            print(opt)

        # Get user answer
        user_ans = input("Enter your choice (A/B/C/D): ").upper()

        # Check answer
        if user_ans == q["answer"]:
            print("✅ Correct!")
            score += 1
        else:
            print(f"❌ Wrong! Correct answer is {q['answer']}")

    # Final Score
    print("\n===== Quiz Completed =====")
    print(f"Your Score: {score} / {len(questions)}")
    percentage = (score / len(questions)) * 100
    print(f"Percentage: {percentage:.2f}%")

    # Performance feedback
    if percentage == 100:
        print("🏆 Excellent! You nailed it!")
    elif percentage >= 60:
        print("👍 Good job, keep practicing!")
    else:
        print("😟 Needs improvement. Keep learning!")

# Step 3: Run the quiz
if __name__ == "__main__":
    run_quiz()
