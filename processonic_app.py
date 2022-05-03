from tkinter import *
from tkinter import filedialog
from PIL import ImageTk, Image
import processonic as ps


def processonic():
    """
    The graphical user interface for Processonic
    :return: None
    """
    source_path = ''
    destination_path = ''
    size_type = 'MB'

    def ask_source_dir():
        global source_path
        source_path = filedialog.askdirectory()
        source_entry.delete(first=0, last=len(source_entry.get()))
        source_entry.insert(0, source_path)

    def ask_destination_dir():
        global destination_path
        destination_path = filedialog.askdirectory()
        destination_entry.delete(first=0, last=len(destination_entry.get()))
        destination_entry.insert(0, destination_path)

    def selected_size_type():
        global size_type
        size_type = selected.get()
        size_type_entry.delete(first=0, last=2)
        size_type_entry.insert(0, size_type)


    def convert_size(threshold):
        global size_type

        if size_type == 'KB':
            return threshold*10**3
        elif size_type == 'MB':
            return threshold*10**6
        elif size_type == 'GB':
            return threshold*10**9


    def task_one():
        threshold = convert_size(int(threshold_entry.get()))
        ps.task_one(source_entry.get(), destination_entry.get(), threshold)

    def task_two():
        ps.task_two(source_entry.get(), destination_entry.get())

    root = Tk()
    HEIGHT = 346
    WIDTH = 600

    canvas = Canvas(root, height=HEIGHT, width=WIDTH)
    canvas.pack()

    background_image = ImageTk.PhotoImage(Image.open('bg.jpg'))
    background_label = Label(root, image=background_image)
    background_label.place(relwidth=1, relheight=1)

    upper_frame = Frame(root, bg='gray', bd=5)
    upper_frame.place(relx=0.5, rely=0.1, relwidth=0.75, relheight=0.1, anchor='n')

    middle_upper_frame = Frame(root, bg='gray', bd=5)
    middle_upper_frame.place(relx=0.5, rely=0.225, relwidth=0.75, relheight=0.1, anchor='n')

    middle_middle_frame = Frame(root, bg='gray', bd=5)
    middle_middle_frame.place(relx=0.5, rely=0.35, relwidth=0.75, relheight=0.1, anchor='n')

    middle_lower_frame = Frame(root, bg='gray', bd=5)
    middle_lower_frame.place(relx=0.5, rely=0.475, relwidth=0.75, relheight=0.1, anchor='n')

    source_entry = Entry(upper_frame, font=40)
    source_entry.place(relwidth=0.65, relheight=1)
    source_entry.insert(0, "Source directory path")

    destination_entry = Entry(middle_upper_frame, font=40)
    destination_entry.place(relwidth=0.65, relheight=1)
    destination_entry.insert(0, "Destination directory path")

    threshold_entry = Entry(middle_middle_frame, font=40)
    threshold_entry.place(relwidth=0.6, relheight=1)
    threshold_entry.insert(0, "Threshold size (default 10 MB)")
    
    size_type_entry = Entry(middle_middle_frame, font=40)
    size_type_entry.place(relx=0.54 ,relwidth=0.06, relheight=1)

    selected = StringVar()
    Radiobutton(middle_middle_frame, text='GB', value='GB', variable=selected,
                                 command=selected_size_type).place(relx=0.65)
    Radiobutton(middle_middle_frame, text='MB', value='MB', variable=selected,
                                 command=selected_size_type).place(relx=0.775)
    Radiobutton(middle_middle_frame, text='KB', value='KB', variable=selected,
                                 command=selected_size_type).place(relx=0.9)

    select_source_button = Button(upper_frame, text='Select directory', font=40,
                                  command=lambda: ask_source_dir())
    select_source_button.place(relx=0.7, relheight=1, relwidth=0.3)

    select_destination_button = Button(middle_upper_frame, text='Select directory', font=40,
                                       command=lambda: ask_destination_dir())
    select_destination_button.place(relx=0.7, relheight=1, relwidth=0.3)

    task_one_button = Button(middle_lower_frame, text='Task One', font=40,
                             command=lambda: task_one())
    task_one_button.place(relx=0, relheight=1, relwidth=0.49)

    task_two_button = Button(middle_lower_frame, text='Task Two', font=40, command=lambda: task_two())
    task_two_button.place(relx=0.51, relheight=1, relwidth=0.49)

    lower_frame = Frame(root, bg='gray', bd=10)
    lower_frame.place(relx=0.5, rely=0.6, relwidth=0.75, relheight=0.3, anchor='n')

    label = Label(lower_frame)
    label.place(relwidth=1, relheight=1)
    root.mainloop()


processonic()