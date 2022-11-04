"""
Bot main code
"""

import os
import string
import sys
import tempfile
import typing
from dataclasses import dataclass, field
from sqlite3 import connect, Connection
import hashlib


from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters, ConversationHandler
import pandas as pd
import dataframe_image

cur_dir = os.path.dirname(__file__)
if cur_dir not in sys.path:
    sys.path.append(cur_dir)

from bot_secrets import API_KEY, SALT
from version import __version__

DB_LOCATION = os.path.abspath(os.path.join(__file__, "..", "data", "bot_db.sqlite"))


@dataclass
class DB:
    db_location: str
    _db_conn: Connection = field(init=False, default=None)

    def __post_init__(self):
        db_exists = os.path.exists(self.db_location)
        self._db_conn = connect(self.db_location)

        if not db_exists:
            self.create_tables()

    def __enter__(self):
        return self

    def close_connection(self) -> None:
        # noinspection PyBroadException
        try:
            self.query("COMMIT", safe=False)
        except Exception:
            pass
        self._db_conn.close()
        return None

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_connection()
        return None

    def create_tables(self) -> None:
        self.query("""
           CREATE TABLE DUMMY
           (
           A int
           )
           """, raise_on_error=False)

        return None

    @staticmethod
    def user_table(chat_id: int) -> str:
        h = (hashlib.sha256(f"{chat_id}_{SALT}".encode('utf-8')).hexdigest())
        res = f"_user_data_{h}"
        return res

    def upload_df(self, df: pd.DataFrame, chat_id: int) -> None:
        df.to_sql(self.user_table(chat_id), self._db_conn, if_exists="append", index=False)
        return None

    def query(
            self,
            query_text: str,
            chat_id: int = None,
            params: typing.Dict[str, typing.Any] = None,
            safe: bool = False,
            raise_on_error: bool = True,
    ) -> typing.Optional[pd.DataFrame]:
        params = params or {}
        if chat_id:
            table_name = self.user_table(chat_id)
            query_text = query_text.replace(":tbl", table_name)
        try:
            if not safe:
                res = pd.read_sql(query_text, self._db_conn, params=params)
            else:
                res = self._db_conn.execute(query_text, params)
        except Exception as e:
            if raise_on_error:
                raise e
            exc_txt = str(e)
            if exc_txt == "'NoneType' object is not iterable":
                return None
            res = pd.DataFrame([{"Exception_text": exc_txt}])
        return res


def upload_csv(update: Update, context: CallbackContext) -> None:
    chat_id = update.message.chat_id

    fn = update.message.document.file_name

    meth = {
        fn.endswith(".csv"): pd.read_csv,
        fn.endswith(".xlsx"): pd.read_excel,
        fn.endswith(".xls"): pd.read_excel,
    }.get(True, None)
    if meth is None:
        raise TypeError("Unsupported file type: {}".format(fn.split(".")[-1]))

    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        context.bot.get_file(update.message.document).download(out=tmp)
        df = meth(tmp)
        with DB(DB_LOCATION) as db:
            db.upload_df(df, chat_id)
        update.message.reply_text("Uploaded successfully")
    except Exception as e:
        print(e)
        raise
    finally:
        tmp.close()
        os.unlink(tmp.name)

    return None


# noinspection PyUnusedLocal
def info(update: Update, context: CallbackContext) -> None:
    msg = "SQLite interface bot version {}"
    msg = msg.format(
        __version__,
    )
    update.message.reply_text(msg)
    return None


ESCAPE_CHARS = string.punctuation


# noinspection PyUnusedLocal
def query(update: Update, context: CallbackContext) -> None:
    chat_id = update.message.chat_id

    q_txt = update.message.text
    with DB(DB_LOCATION) as db:
        res = db.query(q_txt, chat_id=chat_id)

    df_styled = res.style.background_gradient()  # adding a gradient based on values in cell
    fname = f"data/mytable_{chat_id}.png"
    dataframe_image.export(df_styled, fname)
    with open(fname, 'rb') as f:
        update.message.reply_photo(photo=f)
    return None


# noinspection PyUnusedLocal
def error_handler(update: Update, context: CallbackContext) -> None:
    errmsg = f"Error in chat {update.message.chat_id}: {{{context.error.__class__}}} {context.error}"
    print(errmsg)
    update.message.reply_text(f"{errmsg} {{{context.error.__class__}}} {context.error}")
    return None


def main():
    updater = Updater(API_KEY, workers=1)

    updater.dispatcher.add_handler(CommandHandler('version', info))

    updater.dispatcher.add_handler(MessageHandler(Filters.document, upload_csv))

    updater.dispatcher.add_handler(MessageHandler(Filters.text, query))

    updater.dispatcher.add_error_handler(error_handler)

    updater.start_polling()

    updater.idle()
    return None


if __name__ == '__main__':
    main()
