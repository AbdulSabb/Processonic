from tkinter import *
from tkinter import filedialog
from PIL import ImageTk, Image
import processonic as ps
from tqdm import tqdm


def processonic():
    """
    The graphical user interface for Processonic.

    :return: None
    """
    source_path = ''
    destination_path = ''
    size_type = 'MB'

    def ask_source_dir():
        """
        Asks the user to choose the source directory in the operating system.

        :return: None
        """
        global source_path
        source_path = filedialog.askdirectory()
        source_entry.delete(first=0, last=len(source_entry.get()))
        source_entry.insert(0, source_path)

    def ask_destination_dir():
        """
        Asks the user to choose the destination directory in the operating system.

        :return: None
        """
        global destination_path
        destination_path = filedialog.askdirectory()
        destination_entry.delete(first=0, last=len(destination_entry.get()))
        destination_entry.insert(0, destination_path)

    def selected_size_type():
        """
        Stores the value of the selected size type from the user to the size_type variable.

        :return: None
        """
        global size_type

        size_type = selected.get()
        size_type_entry.delete(first=0, last=2)
        size_type_entry.insert(0, size_type)


    def get_converted_size(size):
        """
        Returns the size in bytes after conversion.
        :param size: float
            Size specified by the user.
        :return: int
            Size in bytes after conversion.
        """
        global size_type

        if size_type == 'KB':
            return int(size * 10 ** 3)
        elif size_type == 'MB':
            return int(size * 10 ** 6)
        elif size_type == 'GB':
            return int(size * 10 ** 9)


    def task_one():
        """
        Does task_one(source, destination, threshold) specified in processonic.py.

        :return: None
        """
        threshold = get_converted_size(int(threshold_entry.get()))
        ps.task_one(source_entry.get(), destination_entry.get(), threshold)

    def task_two():
        """
        Does task_two(source, destination) specified in processonic.py.

        :return: None
        """
        ps.task_two(source_entry.get(), destination_entry.get())

    root = Tk()
    HEIGHT = 346
    WIDTH = 600

    canvas = Canvas(root, height=HEIGHT, width=WIDTH)
    canvas.pack()

    background_label = Label(root, bg='black',)
    background_label.place(relwidth=1, relheight=1)

    upper_frame = Frame(root, bg='gray', bd=5)
    upper_frame.place(relx=0.5, rely=0.1, relwidth=0.75, relheight=0.1, anchor='n')

    middle_upper_frame = Frame(root, bg='gray', bd=5)
    middle_upper_frame.place(relx=0.5, rely=0.225, relwidth=0.75, relheight=0.1, anchor='n')

    middle_middle_frame = Frame(root, bg='gray', bd=5)
    middle_middle_frame.place(relx=0.5, rely=0.35, relwidth=0.75, relheight=0.1, anchor='n')

    middle_lower_frame = Frame(root, bg='gray', bd=5)
    middle_lower_frame.place(relx=0.5, rely=0.475, relwidth=0.75, relheight=0.1, anchor='n')

    source_entry = Entry(upper_frame)
    source_entry.place(relwidth=0.65, relheight=1)
    source_entry.insert(0, "Source directory path")

    destination_entry = Entry(middle_upper_frame)
    destination_entry.place(relwidth=0.65, relheight=1)
    destination_entry.insert(0, "Destination directory path")

    threshold_entry = Entry(middle_middle_frame)
    threshold_entry.place(relwidth=0.6, relheight=1)
    threshold_entry.insert(0, "Threshold size (default 10 MB)")
    
    size_type_entry = Entry(middle_middle_frame, font=20)
    size_type_entry.place(relx=0.54 ,relwidth=0.06, relheight=1)

    selected = StringVar()
    Radiobutton(middle_middle_frame, text='GB', value='GB', variable=selected,
                                 command=selected_size_type).place(relx=0.65)
    Radiobutton(middle_middle_frame, text='MB', value='MB', variable=selected,
                                 command=selected_size_type).place(relx=0.775)
    Radiobutton(middle_middle_frame, text='KB', value='KB', variable=selected,
                                 command=selected_size_type).place(relx=0.9)

    select_source_button = Button(upper_frame, text='Select directory',
                                  command=lambda: ask_source_dir())
    select_source_button.place(relx=0.7, relheight=1, relwidth=0.3)

    select_destination_button = Button(middle_upper_frame, text='Select directory',
                                       command=lambda: ask_destination_dir())
    select_destination_button.place(relx=0.7, relheight=1, relwidth=0.3)

    task_one_button = Button(middle_lower_frame, text='Task One',
                             command=lambda: task_one())
    task_one_button.place(relx=0, relheight=1, relwidth=0.49)

    task_two_button = Button(middle_lower_frame, text='Task Two', command=lambda: task_two())
    task_two_button.place(relx=0.51, relheight=1, relwidth=0.49)

    lower_frame = Frame(root, bg='gray', bd=10)
    lower_frame.place(relx=0.5, rely=0.6, relwidth=0.75, relheight=0.3, anchor='n')

    label = Entry(lower_frame)
    label.place(relwidth=1, relheight=1)
    root.mainloop()


processonic()
