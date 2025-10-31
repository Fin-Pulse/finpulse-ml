v# finpulse-ml
Содержит все, что связано с машинным обучением и прогнозированием. 

## 1. Начало работы (делаем один раз)

```bash
# Клонируем репозиторий
git clone https://github.com/Fin-Pulse/finpulse-back.git
cd finpulse-back

# Проверяем что есть ветки
git branch -a
# Должны увидеть: main, develop
```

## 2. Ежедневный цикл разработки

### Шаг 1: Начало дня - берем свежий код
```bash
# Переключаемся на develop
git checkout develop

# Качаем последние изменения
git pull origin develop
```

### Шаг 2: Создаем свою ветку для задачи
```bash
# Создаем и переключаемся на новую ветку
git checkout -b feat/add-login-form
# или для исправлений: git checkout -b fix/button-color
```

### Шаг 3: Работаем и коммитим
```bash
# После каждого логического изменения:

# Смотрим что изменилось
git status

# Добавляем файлы в коммит
git add .
# или конкретный файл: git add src/components/Login.js

# Создаем коммит
git commit -m "feat: add login form component"

# Продолжаем работать...
git add .
git commit -m "feat: add form validation"
git add . 
git commit -m "style: improve login form design"
git commit -m "fix: change login form"
```

### Шаг 4: Пушим ветку на GitHub
```bash
# Первый пуш - создаем ветку на GitHub
git push -u origin feat/add-login-form

# Дальше можно просто:
git push
```

## 3. Создаем Pull Request

1. Идешь на GitHub в репозиторий
2. Видишь кнопку "Compare & pull request" - жмешь
3. Заполняешь:
   - **Title:** `feat: add login form`
   - **Description:** Что сделал, как тестировать
   - **Reviewers:** Выбираешь 2 коллег
4. Жмешь "Create pull request"

## 4. Process ревью

- Ждешь пока 2 человека поставят "Approve"
- Если есть комментарии - исправляешь в той же ветке:
```bash
# Вносишь изменения
git add .
git commit -m "fix: address review comments"
git push
```

## 5. После одобрения - мердж

На GitHub в PR:
1. Жмешь "Merge pull request"
2. Выбираешь "Squash and merge" (объединяет все коммиты в один)
3. Жмешь "Confirm merge"
4. Удаляешь ветку на GitHub (будет кнопка)

## 6. Чистим локально

```bash
# Возвращаемся на develop
git checkout develop

# Качаем обновленную develop (с нашим мерджем)
git pull origin develop

# Удаляем локальную ветку (она уже не нужна)
git branch -d feat/add-login-form
```

## 7. Начинаем новую задачу

И снова с шага 2! 🔄

## Важные моменты:

- **Всегда** начинай с `git checkout develop && git pull`
- **Никогда** не коммить прямо в develop/main
- **Одна ветка** = одна задача/фича
- **Коммить часто** - каждый час работы
- **Перед PR** убедись что твой код работает

## Если что-то пошло не так:

```bash
# Отменить все непроиндексированные изменения
git checkout .

# Вернуться к последнему коммиту (потеряешь изменения!)
git reset --hard HEAD

# Посмотреть историю коммитов
git log --oneline
```

Вот и весь цикл! Каждый день повторяешь шаги 2-6 для новой задачи.
