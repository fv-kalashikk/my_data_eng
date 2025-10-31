import os
import sys
import re

def read_gitignore(gitignore_path='.gitignore'):
    """Читает .gitignore и возвращает списки исключений"""
    ignore_dirs = []
    ignore_files = []
    ignore_patterns = []
    
    if not os.path.exists(gitignore_path):
        return ignore_dirs, ignore_files, ignore_patterns
    
    with open(gitignore_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Пропускаем пустые строки и комментарии
            if not line or line.startswith('#'):
                continue
            
            # Убираем слеши в конце для папок
            if line.endswith('/'):
                ignore_dirs.append(line.rstrip('/'))
            # Паттерны с wildcards
            elif '*' in line or '?' in line:
                ignore_patterns.append(line)
            # Конкретные файлы
            else:
                ignore_files.append(line)
    
    return ignore_dirs, ignore_files, ignore_patterns

def matches_pattern(filename, patterns):
    """Проверяет, соответствует ли файл любому из паттернов .gitignore"""
    for pattern in patterns:
        if fnmatch.fnmatch(filename, pattern):
            return True
    return False

def generate_project_tree(start_path='.'):
    """Генерирует дерево проекта с учетом .gitignore"""
    
    # Читаем исключения из .gitignore
    gitignore_dirs, gitignore_files, gitignore_patterns = read_gitignore()
    
    # Стандартные исключения (дополняем .gitignore)
    exclude_dirs = set(['.git', '__pycache__', '.ipynb_checkpoints', 'venv'] + gitignore_dirs)
    exclude_files = set(['.DS_Store', '.env'] + gitignore_files)
    
    tree = []
    
    for root, dirs, files in os.walk(start_path):
        # Исключаем ненужные папки
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        level = root.replace(start_path, '').count(os.sep)
        if level == 0:
            indent = ''
        else:
            indent = '│   ' * (level - 1) + '├── '
        
        # Добавляем текущую папку (кроме корневой)
        if root != start_path:
            tree.append(f"{indent}{os.path.basename(root)}/")
        
        subindent = '│   ' * level + '├── '
        
        # Обрабатываем файлы
        for file in files:
            # Пропускаем файлы из исключений
            if file in exclude_files:
                continue
            # Пропускаем файлы, соответствующие паттернам .gitignore
            if matches_pattern(file, gitignore_patterns):
                continue
            # Пропускаем сам этот скрипт и создаваемый файл
            if file in ['generate_tree.py', 'PROJECT_STRUCTURE.md']:
                continue
                
            tree.append(f"{subindent}{file}")
    
    return '\n'.join(tree)

# Добавляем импорт для паттернов
import fnmatch

if __name__ == "__main__":
    print("Чтение .gitignore и генерация структуры проекта...")
    
    project_tree = generate_project_tree('.')
    
    print("Структура проекта (с учетом .gitignore):")
    print(project_tree)
    
    # Сохраняем в файл
    with open('PROJECT_STRUCTURE.md', 'w', encoding='utf-8') as f:
        f.write("# Структура проекта\n\n")
        f.write("Сгенерировано автоматически с учетом `.gitignore`\n\n")
        f.write("```\n")
        f.write(project_tree)
        f.write("\n```")
    
    print("\n✅ Структура сохранена в PROJECT_STRUCTURE.md")